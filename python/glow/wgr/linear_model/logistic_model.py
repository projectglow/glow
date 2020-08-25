# Copyright 2019 The Glow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .logistic_udfs import *
from nptyping import Float, NDArray
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.functions as f
from typeguard import typechecked
from typing import Any, Dict, List
from glow.logging import record_hls_event


@typechecked
class LogisticRegression:
    """
    The LogisticRegression class is used to fit logistic regression models against one or more labels optimized over a
    provided list of ridge alpha parameters. The optimal ridge alpha value is chosen for each label by minimizing the
    average out of fold log_loss scores.
    """
    def __init__(self, alphas: NDArray[(Any, ), Float] = np.array([])) -> None:
        """
        LogisticRegression is initialized with a list of alpha values.

        Args:
            alphas : array_like of alpha values used in the ridge regression (optional).
        """
        if not (alphas >= 0).all():
            raise Exception('Alpha values must all be non-negative.')
        self.alphas = create_alpha_dict(alphas)

    def fit(
        self,
        blockdf: DataFrame,
        labeldf: pd.DataFrame,
        sample_blocks: Dict[str, List[str]],
        covdf: pd.DataFrame = pd.DataFrame({})
    ) -> (DataFrame, DataFrame):
        """
        Fits a logistic regression model, represented by a Spark DataFrame containing coefficients for each of the ridge
        alpha parameters, for each block in the starting matrix, for each label in the target labels, as well as a
        Spark DataFrame containing the optimal ridge alpha value for each label.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            sample_blocks : Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
            covdf : Pandas DataFrame containing covariates to be included in every model in the stacking
                ensemble (optional).  The covariates should not include an explicit intercept term, as one will be
                added automatically.  If empty, the intercept will be used as the only covariate.

        Returns:
            Two Spark DataFrames, one containing the model resulting from the fitting routine and one containing the
            results of the cross validation procedure.
        """
        map_key_pattern = ['sample_block', 'label', 'alpha_name']
        reduce_key_pattern = ['header_block', 'header', 'label', 'alpha_name']
        model_key_pattern = ['sample_block', 'label', 'alpha_name']
        score_key_pattern = ['sample_block', 'label']
        metric = 'log_loss'

        if not self.alphas:
            self.alphas = generate_alphas(blockdf)

        if covdf.empty:
            covdf = pd.DataFrame(data=np.ones(labeldf.shape[0]),
                                 columns=['intercept'],
                                 index=labeldf.index)
            validate_inputs(labeldf, pd.DataFrame({}), 'binary')
        else:
            covdf = covdf.copy()
            validate_inputs(labeldf, covdf, 'binary')
            covdf.insert(0, 'intercept', 1)

        maskdf = pd.DataFrame(data=np.where(np.isnan(labeldf), False, True),
                              columns=labeldf.columns,
                              index=labeldf.index)

        beta_cov_dict = {}
        for label in labeldf:
            row_mask = slice_label_rows(maskdf, label, list(labeldf.index), np.array([])).ravel()
            cov_mat = slice_label_rows(covdf, 'all', list(labeldf.index), row_mask)
            y = slice_label_rows(labeldf, label, list(labeldf.index), row_mask).ravel()
            fit_result = constrained_logistic_fit(cov_mat,
                                                  y,
                                                  np.zeros(cov_mat.shape[1]),
                                                  guess=np.array([]),
                                                  n_cov=0)
            beta_cov_dict[label] = fit_result.x

        map_udf = pandas_udf(
            lambda key, pdf: map_irls_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks, covdf,
                                          beta_cov_dict, maskdf, self.alphas), irls_eqn_struct,
            PandasUDFType.GROUPED_MAP)

        reduce_udf = pandas_udf(lambda key, pdf: reduce_irls_eqn(key, reduce_key_pattern, pdf),
                                irls_eqn_struct, PandasUDFType.GROUPED_MAP)

        model_udf = pandas_udf(
            lambda key, pdf: solve_irls_eqn(key, model_key_pattern, pdf, labeldf, self.alphas, covdf
                                            ), model_struct, PandasUDFType.GROUPED_MAP)

        score_udf = pandas_udf(
            lambda key, pdf: score_models(key, score_key_pattern, pdf, labeldf, sample_blocks, self.
                                          alphas, covdf, maskdf, metric), cv_struct,
            PandasUDFType.GROUPED_MAP)

        modeldf = blockdf.drop('alpha') \
            .withColumn('alpha_name', f.explode(f.array([f.lit(n) for n in self.alphas.keys()]))) \
            .groupBy(map_key_pattern) \
            .apply(map_udf) \
            .groupBy(reduce_key_pattern) \
            .apply(reduce_udf) \
            .groupBy(model_key_pattern) \
            .apply(model_udf) \
            .withColumn('alpha_label_coef', f.expr('struct(alphas[0] AS alpha, labels[0] AS label, coefficients[0] AS coefficient)')) \
            .groupBy('header_block', 'sample_block', 'header', 'sort_key', f.col('alpha_label_coef.label')) \
            .agg(f.sort_array(f.collect_list('alpha_label_coef')).alias('alphas_labels_coefs')) \
            .selectExpr('*', 'alphas_labels_coefs.alpha AS alphas', 'alphas_labels_coefs.label AS labels', 'alphas_labels_coefs.coefficient AS coefficients') \
            .drop('alphas_labels_coefs', 'label')

        cvdf = cross_validation(blockdf, modeldf, score_udf, score_key_pattern, self.alphas, metric)

        record_hls_event('wgrLogisticRegressionFit')

        return modeldf, cvdf

    def reduce_block_matrix(self, blockdf: DataFrame, labeldf: pd.DataFrame,
                            sample_blocks: Dict[str, List[str]], modeldf: DataFrame,
                            cvdf: DataFrame, covdf: pd.DataFrame, response: str) -> DataFrame:
        """
        Transforms a starting block matrix by applying a linear model.  The form of the output
        can either be a direct linear transformation (response = "linear") or a linear transformation followed by a
        sigmoid transformation (response = "sigmoid").

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            sample_blocks : Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
            modeldf : Spark DataFrame produced by the LogisticRegression fit method, representing the reducer model
            cvdf : Spark DataFrame produced by the LogisticRegression fit method, containing the results of the cross
            validation routine.
            covdf : Pandas DataFrame containing covariates to be included in every model in the stacking
                ensemble.  The covariates should not include an explicit intercept term, as one will be
                added automatically.
            response : String specifying what transformation to apply ("linear" or "sigmoid")

        Returns:
            Spark DataFrame containing the result of the transformation.
        """

        transform_key_pattern = ['sample_block', 'label']

        if response == 'linear':
            transform_udf = pandas_udf(
                lambda key, pdf: apply_model(key, transform_key_pattern, pdf, labeldf,
                                             sample_blocks, self.alphas, covdf),
                reduced_matrix_struct, PandasUDFType.GROUPED_MAP)
            join_type = 'inner'
        elif response == 'sigmoid':
            if covdf.empty:
                covdf = pd.DataFrame(data=np.ones(labeldf.shape[0]),
                                     columns=['intercept'],
                                     index=labeldf.index)
            else:
                covdf = covdf.copy()
                covdf.insert(0, 'intercept', 1)
            transform_udf = pandas_udf(
                lambda key, pdf: apply_logistic_model(key, transform_key_pattern, pdf, labeldf,
                                                      sample_blocks, self.alphas, covdf),
                logistic_reduced_matrix_struct, PandasUDFType.GROUPED_MAP)
            join_type = 'right'
        else:
            raise ValueError(f'response must be either "linear" or "sigmoid", received "{response}"')

        return blockdf.drop('header_block', 'sort_key') \
            .join(modeldf.drop('header_block'), ['sample_block', 'header'], join_type) \
            .withColumn('label', f.coalesce(f.col('label'), f.col('labels').getItem(0))) \
            .groupBy(transform_key_pattern) \
            .apply(transform_udf) \
            .join(cvdf, ['label', 'alpha'], 'inner')

    def transform(self,
                  blockdf: DataFrame,
                  labeldf: pd.DataFrame,
                  sample_blocks: Dict[str, List[str]],
                  modeldf: DataFrame,
                  cvdf: DataFrame,
                  covdf: pd.DataFrame = pd.DataFrame({}),
                  response: str = 'linear') -> pd.DataFrame:
        """
        Generates GWAS covariates for the target labels in the provided label DataFrame by applying the model resulting
        from the LogisticRegression fit method to the starting block matrix.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            sample_blocks : Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
            modeldf : Spark DataFrame produced by the LogisticRegression fit method, representing the reducer model
            cvdf : Spark DataFrame produced by the LogisticRegression fit method, containing the results of the cross
            validation routine.
            covdf : Pandas DataFrame containing covariates to be included in every model in the stacking
                ensemble (optional). The covariates should not include an explicit intercept term, as one will be
                added automatically.
            response : String specifying the desired output.  Can be 'linear' to specify the direct output of the linear
                WGR model (default) or 'sigmoid' to specify predicted label probabilities.

        Returns:
            Pandas DataFrame containing  covariate values. The shape and order match labeldf such that the
            rows are indexed by sample ID and the columns by label. The column types are float64.
        """

        block_prediction_df = self.reduce_block_matrix(blockdf, labeldf, sample_blocks, modeldf,
                                                       cvdf, covdf, response)
        pivoted_df = flatten_prediction_df(block_prediction_df, sample_blocks, labeldf)

        record_hls_event('wgrLogisticRegressionTransform')

        return pivoted_df

    def transform_loco(self,
                       blockdf: DataFrame,
                       labeldf: pd.DataFrame,
                       sample_blocks: Dict[str, List[str]],
                       modeldf: DataFrame,
                       cvdf: DataFrame,
                       covdf: pd.DataFrame = pd.DataFrame({}),
                       response: str = 'linear',
                       chromosomes: List[str] = []) -> pd.DataFrame:
        """
        Generates predictions for the target labels in the provided label DataFrame by applying the model resulting from
        the RidgeRegression fit method to the starting block matrix using a leave-one-chromosome-out (LOCO) approach.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            sample_blocks : Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
            modeldf : Spark DataFrame produced by the RidgeRegression fit method, representing the reducer model
            cvdf : Spark DataFrame produced by the RidgeRegression fit method, containing the results of the cross
            validation routine.
            covdf : covdf : Pandas DataFrame containing covariates to be included in every model in the stacking
                ensemble (optional). The covariates should not include an explicit intercept term, as one will be
                added automatically.
            response : String specifying the desired output.  Can be 'linear' to specify the direct output of the linear
                WGR model (default) or 'sigmoid' to specify predicted label probabilities.
            chromosomes : List of chromosomes for which to generate a prediction (optional). If not provided, the
            chromosomes will be inferred from the block matrix.

        Returns:
            Pandas DataFrame containing prediction y_hat values per chromosome. The rows are indexed by sample ID and
            chromosome; the columns are indexed by label. The column types are float64. The DataFrame is sorted using
            chromosome as the primary sort key, and sample ID as the secondary sort key.
        """
        loco_chromosomes = chromosomes if chromosomes else infer_chromosomes(blockdf)
        loco_chromosomes.sort()

        all_y_hat_df = pd.DataFrame({})
        for chromosome in loco_chromosomes:
            loco_model_df = modeldf.filter(
                ~f.col('header').rlike(f'^chr_{chromosome}_(alpha|block)'))
            loco_y_hat_df = self.transform(blockdf, labeldf, sample_blocks, loco_model_df, cvdf,
                                           covdf, response)
            loco_y_hat_df['contigName'] = chromosome
            all_y_hat_df = all_y_hat_df.append(loco_y_hat_df)
        return all_y_hat_df.set_index('contigName', append=True)

    def fit_transform(self,
                      blockdf: DataFrame,
                      labeldf: pd.DataFrame,
                      sample_blocks: Dict[str, List[str]],
                      covdf: pd.DataFrame = pd.DataFrame({}),
                      response: str = 'linear') -> pd.DataFrame:
        """
        Fits a logistic regression model with a block matrix, then transforms the matrix using the model.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            sample_blocks : Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
            covdf : Pandas DataFrame containing covariates to be included in every model in the stacking
                ensemble (optional). The covariates should not include an explicit intercept term, as one will be
                added automatically.
            response : String specifying the desired output.  Can be 'linear' to specify the direct output of the linear
                WGR model (default) or 'sigmoid' to specify predicted label probabilities.

        Returns:
            Pandas DataFrame containing prediction y_hat values. The shape and order match labeldf such that the
            rows are indexed by sample ID and the columns by label. The column types are float64.
        """
        modeldf, cvdf = self.fit(blockdf, labeldf, sample_blocks, covdf)
        if response == 'linear':
            return self.transform(blockdf, labeldf, sample_blocks, modeldf, cvdf, pd.DataFrame({}),
                                  response)
        else:
            return self.transform(blockdf, labeldf, sample_blocks, modeldf, cvdf, covdf, response)
