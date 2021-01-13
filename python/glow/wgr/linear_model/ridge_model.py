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

from .ridge_udfs import *
from nptyping import Float, NDArray
import pandas as pd
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.functions as f
from typeguard import typechecked
from typing import Any, Dict, List
from glow.logging import record_hls_event
import warnings

# Ignore warning to use applyInPandas instead of apply
# TODO(hhd): Remove this and start using applyInPandas once we only support Spark 3.x.
warnings.filterwarnings('ignore', category=UserWarning, message='.*applyInPandas.*')

__all__ = ['RidgeReduction', 'RidgeRegression']


@typechecked
class RidgeReduction:
    """
    The RidgeReducer class is intended to reduce the feature space of an N by M block matrix X to an N by P<<M block
    matrix.  This is done by fitting K ridge models within each block of X on one or more target labels, such that a
    block with L columns to begin with will be reduced to a block with K columns, where each column is the prediction
    of one ridge model for one target label.
    """

    def __init__(self,
                 blockdf: DataFrame,
                 labeldf: pd.DataFrame,
                 sample_blocks: Dict[str, List[str]],
                 covdf: pd.DataFrame = pd.DataFrame({}),
                 alphas: NDArray[(Any,), Float] = np.array([]),
                 label_type='detect') -> None:
        """
        RidgeReducer is initialized with a list of alpha values.

        Args:
            is_binary:
            alphas : array_like of alpha values used in the ridge reduction (optional).
        """
        self.blockdf = blockdf
        self.__blockdf_id = id(self.blockdf)

        self.reduced_blockdf = blockdf
        self.__reduced_blockdf_id = id(self.reduced_blockdf)

        self.modeldf = None
        self.__modeldf_id = id(self.modeldf)

        self.labeldf = prepare_labels_and_warn(labeldf, label_type)

        self.sample_blocks = sample_blocks

        self.covdf = prepare_covariates(covdf)

        if not (alphas >= 0).all():
            raise Exception('Alpha values must all be non-negative.')
        self.alphas = create_alpha_dict(alphas)

    def __getstate__(self):
        # Copy the object's state from self.__dict__ which contains
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state['blockdf'], state['modeldf']
        return state

    def __setstate__(self, state):
        # Restore instance attributes
        self.__dict__.update(state)
        self.blockdf = [x for x in globals().values() if id(x) == self.__blockdf_id]
        self.modeldf = [x for x in globals().values() if id(x) == self.__modeldf_id]

    def fit(self) -> DataFrame:
        """
        Fits a ridge reducer model, represented by a Spark DataFrame containing coefficients for each of the ridge
        alpha parameters, for each block in the starting matrix, for each label in the target labels.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            sample_blocks : Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
            covdf : Pandas DataFrame containing covariates to be included in every model in the stacking
                ensemble (optional).

        Returns:
            Spark DataFrame containing the model resulting from the fitting routine.
        """

        map_key_pattern = ['header_block', 'sample_block']
        reduce_key_pattern = ['header_block', 'header']

        if 'label' in self.blockdf.columns:
            map_key_pattern.append('label')
            reduce_key_pattern.append('label')
        if not self.alphas:
            self.alphas = generate_alphas(self.blockdf)

        map_udf = pandas_udf(
            lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, self.labeldf, self.sample_blocks, self.covdf
                                            ), normal_eqn_struct, PandasUDFType.GROUPED_MAP)
        reduce_udf = pandas_udf(lambda key, pdf: reduce_normal_eqn(key, reduce_key_pattern, pdf),
                                normal_eqn_struct, PandasUDFType.GROUPED_MAP)
        model_udf = pandas_udf(
            lambda key, pdf: solve_normal_eqn(key, map_key_pattern, pdf, self.labeldf, self.alphas, self.covdf
                                              ), model_struct, PandasUDFType.GROUPED_MAP)

        record_hls_event('wgrRidgeReduceFit')

        self.modeldf = self.blockdf \
            .groupBy(map_key_pattern) \
            .apply(map_udf) \
            .groupBy(reduce_key_pattern) \
            .apply(reduce_udf) \
            .groupBy(map_key_pattern) \
            .apply(model_udf)

        self.__modeldf_id = id(self.modeldf)

        return self.modeldf

    def transform(self, external_modeldf: DataFrame = None) -> DataFrame:
        """
        Transforms a starting block matrix to the reduced block matrix, using a reducer model produced by the
        RidgeReducer fit method.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            sample_blocks: Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
            modeldf : Spark DataFrame produced by the RidgeReducer fit method, representing the reducer model
            covdf : Pandas DataFrame containing covariates to be included in every model in the stacking
                ensemble (optional).

        Returns:
             Spark DataFrame representing the reduced block matrix
        """
        working_modeldf = self.modeldf if external_modeldf is None else external_modeldf

        if working_modeldf is None:
            raise ValueError(
                'No model DataFrame found! Either run the fit function first or provide a previously made model DataFrame.')

        transform_key_pattern = ['header_block', 'sample_block']

        if 'label' in self.blockdf.columns:
            transform_key_pattern.append('label')
            joined = self.blockdf.drop('sort_key') \
                .join(working_modeldf, ['header_block', 'sample_block', 'header'], 'right') \
                .withColumn('label', f.coalesce(f.col('label'), f.col('labels').getItem(0)))
        else:
            joined = self.blockdf.drop('sort_key') \
                .join(working_modeldf, ['header_block', 'sample_block', 'header'], 'right')

        transform_udf = pandas_udf(
            lambda key, pdf: apply_model(key, transform_key_pattern, pdf, self.labeldf, self.sample_blocks,
                                         self.alphas, self.covdf), reduced_matrix_struct,
            PandasUDFType.GROUPED_MAP)

        record_hls_event('wgrRidgeReduceTransform')

        self.reduced_blockdf = joined \
            .groupBy(transform_key_pattern) \
            .apply(transform_udf)

        self.__reduced_blockdf_id = id(self.reduced_blockdf)

        return self.reduced_blockdf

    def fit_transform(self) -> DataFrame:
        """
        Fits a ridge reducer model with a block matrix, then transforms the matrix using the model.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            sample_blocks : Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
            covdf : Pandas DataFrame containing covariates to be included in every model in the stacking
                    ensemble (optional).

        Returns:
            Spark DataFrame representing the reduced block matrix
        """

        self.fit()
        return self.transform()


@typechecked
class RidgeRegression:
    """
    The RidgeRegression class is used to fit ridge models against one or more labels optimized over a provided list of
    ridge alpha parameters.  It is similar in function to RidgeReducer except that whereas RidgeReducer attempts to
    reduce a starting matrix X to a block matrix of smaller dimension, RidgeRegression is intended to find an optimal
    model of the form Y_hat ~ XB, where Y_hat is a matrix of one or more predicted labels and B is a matrix of
    coefficients.  The optimal ridge alpha value is chosen for each label by maximizing the average out of fold r2
    score.
    """

    def __init__(self,
                 ridge_reduced: RidgeReduction,
                 alphas: NDArray[(Any,), Float] = np.array([])) -> None:
        """
        RidgeRegression is initialized with a list of alpha values.

        Args:
            alphas : array_like of alpha values used in the ridge regression (optional).
        """
        self.reduced_blockdf = ridge_reduced.reduced_blockdf
        self.__reduced_blockdf_id = ridge_reduced.__reduced_blockdf_id

        self.modeldf = None
        self.__modeldf_id = id(self.modeldf)

        self.cvdf = None
        self.__cvdf_id = id(self.cvdf)

        self.labeldf = ridge_reduced.labeldf

        self.sample_blocks = ridge_reduced.sample_blocks

        self.covdf = ridge_reduced.covdf

        self.y_hat_df = None

        if not (alphas >= 0).all():
            raise Exception('Alpha values must all be non-negative.')
        self.alphas = create_alpha_dict(alphas)


    def fit(self) -> (DataFrame, DataFrame):
        """
        Fits a ridge regression model, represented by a Spark DataFrame containing coefficients for each of the ridge
        alpha parameters, for each block in the starting matrix, for each label in the target labels, as well as a
        Spark DataFrame containing the optimal ridge alpha value for each label.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            sample_blocks : Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
            covdf : Pandas DataFrame containing covariates to be included in every model in the stacking
                ensemble (optional).

        Returns:
            Two Spark DataFrames, one containing the model resulting from the fitting routine and one containing the
            results of the cross validation procedure.
        """

        map_key_pattern = ['sample_block', 'label']
        reduce_key_pattern = ['header_block', 'header', 'label']
        metric = 'r2'

        if not self.alphas:
            self.alphas = generate_alphas(self.reduced_blockdf)

        map_udf = pandas_udf(
            lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, self.labeldf, self.sample_blocks, self.covdf
                                            ), normal_eqn_struct, PandasUDFType.GROUPED_MAP)
        reduce_udf = pandas_udf(lambda key, pdf: reduce_normal_eqn(key, reduce_key_pattern, pdf),
                                normal_eqn_struct, PandasUDFType.GROUPED_MAP)
        model_udf = pandas_udf(
            lambda key, pdf: solve_normal_eqn(key, map_key_pattern, pdf, self.labeldf, self.alphas, self.covdf
                                              ), model_struct, PandasUDFType.GROUPED_MAP)
        score_udf = pandas_udf(
            lambda key, pdf: score_models(key, map_key_pattern, pdf, self.labeldf, self.sample_blocks, self.
                                          alphas, self.covdf, pd.DataFrame({}), metric), cv_struct,
            PandasUDFType.GROUPED_MAP)

        self.modeldf = self.reduced_blockdf \
            .groupBy(map_key_pattern) \
            .apply(map_udf) \
            .groupBy(reduce_key_pattern) \
            .apply(reduce_udf) \
            .groupBy(map_key_pattern) \
            .apply(model_udf)

        self.__modeldf_id = id(self.modeldf)

        self.cvdf = cross_validation(self.reduced_blockdf, self.modeldf, score_udf, map_key_pattern, self.alphas, metric)

        self.__cvdf_id = id(self.cvdf)

        record_hls_event('wgrRidgeRegressionFit')

        return self.modeldf, self.cvdf

    def __rectify_transform_inputs(self,
                                   external_modeldf: DataFrame,
                                   external_cvdf: DataFrame) -> (DataFrame, DataFrame):

        working_modeldf = self.modeldf if external_modeldf is None else external_modeldf

        if working_modeldf is None:
            raise ValueError(
                'No model DataFrame found! Either run the fit function first or provide a previously made model DataFrame.')

        working_cvdf = self.cvdf if external_cvdf is None else external_cvdf

        if working_cvdf is None:
            raise ValueError(
                'No cross validation DataFrame found! Either run the fit function first or provide a previously made cv DataFrame.')

    def transform(self,
                  external_modeldf: DataFrame = None,
                  external_cvdf: DataFrame = None) -> pd.DataFrame:
        """
        Generates predictions for the target labels in the provided label DataFrame by applying the model resulting from
        the RidgeRegression fit method to the starting block matrix.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            sample_blocks : Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
            modeldf : Spark DataFrame produced by the RidgeRegression fit method, representing the reducer model
            cvdf : Spark DataFrame produced by the RidgeRegression fit method, containing the results of the cross
                validation routine.
            covdf : Pandas DataFrame containing covariates to be included in every model in the stacking
                ensemble (optional).

        Returns:
            Pandas DataFrame containing prediction y_hat values. The shape and order match labeldf such that the
            rows are indexed by sample ID and the columns by label. The column types are float64.
        """
        working_modeldf, working_cvdf = self.__rectify_transform_inputs(external_modeldf, external_cvdf)

        transform_key_pattern = ['sample_block', 'label']

        transform_udf = pandas_udf(
            lambda key, pdf: apply_model(key, transform_key_pattern, pdf, self.labeldf, self.sample_blocks,
                                         self.alphas, self.covdf), reduced_matrix_struct,
            PandasUDFType.GROUPED_MAP)

        blocked_prediction_df = apply_model_df(self.reduced_blockdf, working_modeldf, working_cvdf, transform_udf,
                                               transform_key_pattern, 'right')

        pivoted_df = flatten_prediction_df(blocked_prediction_df, self.sample_blocks, self.labeldf)

        record_hls_event('wgrRidgeRegressionTransform')

        return pivoted_df

    def transform_loco(self,
                       external_modeldf: DataFrame = None,
                       external_cvdf: DataFrame = None,
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
            covdf : Pandas DataFrame containing covariates to be included in every model in the stacking
                ensemble (optional).
            chromosomes : List of chromosomes for which to generate a prediction (optional). If not provided, the
                chromosomes will be inferred from the block matrix.

        Returns:
            Pandas DataFrame containing prediction y_hat values per chromosome. The rows are indexed by sample ID and
            chromosome; the columns are indexed by label. The column types are float64. The DataFrame is sorted using
            chromosome as the primary sort key, and sample ID as the secondary sort key.
        """
        working_modeldf, working_cvdf = self.__rectify_transform_inputs(external_modeldf, external_cvdf)

        loco_chromosomes = chromosomes if chromosomes else infer_chromosomes(self.reduced_blockdf)
        loco_chromosomes.sort()

        y_hat_df = pd.DataFrame({})
        for chromosome in loco_chromosomes:
            print(f"Generating predictions for chromosome {chromosome}.")
            loco_model_df = working_modeldf.filter(
                ~f.col('header').rlike(f'^chr_{chromosome}_(alpha|block)'))
            loco_y_hat_df = self.transform(loco_model_df, working_cvdf)
            loco_y_hat_df['contigName'] = chromosome
            y_hat_df = y_hat_df.append(loco_y_hat_df)

        self.y_hat_df = y_hat_df.set_index('contigName', append=True)
        return self.y_hat_df

    def fit_transform(self) -> pd.DataFrame:
        """
        Fits a ridge regression model with a block matrix, then transforms the matrix using the model.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            sample_blocks : Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
            covdf : Pandas DataFrame containing covariates to be included in every model in the stacking
                ensemble (optional).

        Returns:
            Pandas DataFrame containing prediction y_hat values. The shape and order match labeldf such that the
            rows are indexed by sample ID and the columns by label. The column types are float64.
        """
        self.fit()
        return self.transform()
