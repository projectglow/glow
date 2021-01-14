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
from .ridge_reduction import RidgeReduction
from nptyping import Float, NDArray
import pandas as pd
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.functions as f
from typeguard import typechecked
from typing import Any, Dict, List, Union
from glow.logging import record_hls_event
import warnings

# Ignore warning to use applyInPandas instead of apply
# TODO(hhd): Remove this and start using applyInPandas once we only support Spark 3.x.
warnings.filterwarnings('ignore', category=UserWarning, message='.*applyInPandas.*')

__all__ = ['RidgeRegression']


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
    def __init__(
        self, ridge_reduced: RidgeReduction, alphas: NDArray[(Any, ),
                                                             Float] = np.array([])) -> None:
        """
        Args:
            ridge_reduced: RidgeReduction object containing level 0 reduction data
            alphas : array_like of alpha values used in the ridge regression (optional).
        """
        self.__set_reduced_block_df(ridge_reduced.get_reduced_block_df())
        self.__sample_blocks = ridge_reduced.get_sample_blocks()
        self.__std_label_df = ridge_reduced.get_std_label_df()
        self.__std_cov_df = ridge_reduced.get_std_cov_df()
        self.set_alphas(alphas)
        self.set_model_df()
        self.set_cv_df()
        self.__y_hat_df = None

    def __getstate__(self):
        # Copy the object's state from self.__dict__ which contains
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state['_RidgeRegression__reduced_block_df'], state['_RidgeRegression__model_df'], state[
            '_RidgeRegression__cv_df']
        return state

    def __setstate__(self, state):
        # Restore instance attributes
        self.__dict__.update(state)
        self.__reduced_block_df = [
            x for x in globals().values() if id(x) == self.__reduced_block_df_id
        ]
        self.__model_df = [x for x in globals().values() if id(x) == self.__model_df_id]
        self.__cv_df = [x for x in globals().values() if id(x) == self.__cv_df_id]

    def __set_reduced_block_df(self, reduced_block: DataFrame) -> None:
        self.__reduced_block_df = reduced_block
        self.__reduced_block_df_id = id(self.__reduced_block_df)

    def set_alphas(self, alphas: NDArray[(Any, ), Float]) -> None:
        self.__alphas = generate_alphas(
            self.__reduced_block_df) if alphas.size == 0 else create_alpha_dict(alphas)

    def set_model_df(self, model_df: DataFrame = None) -> None:
        self.__model_df = model_df
        self.__model_df_id = id(self.__model_df)

    def set_cv_df(self, cv_df: DataFrame = None) -> None:
        self.__cv_df = cv_df
        self.__cv_df_id = id(self.__cv_df)

    def get_alphas(self) -> Dict[str, Float]:
        return self.__alphas

    def get_model_df(self) -> DataFrame:
        return self.__model_df

    def get_cv_df(self) -> DataFrame:
        return self.__cv_df

    def get_y_hat_df(self) -> pd.DataFrame:
        return self.__y_hat_df

    def fit(self) -> (DataFrame, DataFrame):
        """
        Fits a ridge regression model, represented by a Spark DataFrame containing coefficients for each of the ridge
        alpha parameters, for each block in the starting matrix, for each label in the target labels, as well as a
        Spark DataFrame containing the optimal ridge alpha value for each label.

        Returns:
            Two Spark DataFrames, one containing the model resulting from the fitting routine and one containing the
            results of the cross validation procedure.
        """

        map_key_pattern = ['sample_block', 'label']
        reduce_key_pattern = ['header_block', 'header', 'label']
        metric = 'r2'

        map_udf = pandas_udf(
            lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, self.__std_label_df, self.
                                            __sample_blocks, self.__std_cov_df), normal_eqn_struct,
            PandasUDFType.GROUPED_MAP)
        reduce_udf = pandas_udf(lambda key, pdf: reduce_normal_eqn(key, reduce_key_pattern, pdf),
                                normal_eqn_struct, PandasUDFType.GROUPED_MAP)
        model_udf = pandas_udf(
            lambda key, pdf: solve_normal_eqn(key, map_key_pattern, pdf, self.__std_label_df, self.
                                              __alphas, self.__std_cov_df), model_struct,
            PandasUDFType.GROUPED_MAP)
        score_udf = pandas_udf(
            lambda key, pdf: score_models(key, map_key_pattern, pdf, self.__std_label_df, self.
                                          __sample_blocks, self.__alphas, self.__std_cov_df,
                                          pd.DataFrame({}), metric), cv_struct,
            PandasUDFType.GROUPED_MAP)

        self.set_model_df(
            self.__reduced_block_df.groupBy(map_key_pattern).apply(map_udf).groupBy(
                reduce_key_pattern).apply(reduce_udf).groupBy(map_key_pattern).apply(model_udf))

        self.set_cv_df(
            cross_validation(self.__reduced_block_df, self.__model_df, score_udf, map_key_pattern,
                             self.__alphas, metric))

        record_hls_event('wgrRidgeRegressionFit')

        return self.__model_df, self.__cv_df

    def transform(self) -> pd.DataFrame:
        """
        Generates predictions for the target labels in the provided label DataFrame by applying the model resulting from
        the RidgeRegression fit method to the starting block matrix.

        Returns:
            Pandas DataFrame containing prediction y_hat values. The shape and order match labeldf such that the
            rows are indexed by sample ID and the columns by label. The column types are float64.
        """
        check_model(self.__model_df)
        check_cv(self.__cv_df)

        transform_key_pattern = ['sample_block', 'label']

        transform_udf = pandas_udf(
            lambda key, pdf: apply_model(key, transform_key_pattern, pdf, self.__std_label_df, self.
                                         __sample_blocks, self.__alphas, self.__std_cov_df),
            reduced_matrix_struct, PandasUDFType.GROUPED_MAP)

        blocked_prediction_df = apply_model_df(self.__reduced_block_df, self.__model_df,
                                               self.__cv_df, transform_udf, transform_key_pattern,
                                               'right')

        self.__y_hat_df = flatten_prediction_df(blocked_prediction_df, self.__sample_blocks,
                                                self.__std_label_df)

        record_hls_event('wgrRidgeRegressionTransform')

        return self.__y_hat_df

    def transform_loco(self, chromosomes: List[str] = []) -> pd.DataFrame:
        """
        Generates predictions for the target labels in the provided label DataFrame by applying the model resulting from
        the RidgeRegression fit method to the starting block matrix using a leave-one-chromosome-out (LOCO) approach.

        Args:
            chromosomes : List of chromosomes for which to generate a prediction (optional). If not provided, the
                chromosomes will be inferred from the block matrix.

        Returns:
            Pandas DataFrame containing prediction y_hat values per chromosome. The rows are indexed by sample ID and
            chromosome; the columns are indexed by label. The column types are float64. The DataFrame is sorted using
            chromosome as the primary sort key, and sample ID as the secondary sort key.
        """
        loco_chromosomes = chromosomes if chromosomes else infer_chromosomes(
            self.__reduced_block_df)
        loco_chromosomes.sort()

        y_hat_df = pd.DataFrame({})
        orig_model_df = self.__model_df
        for chromosome in loco_chromosomes:
            print(f"Generating predictions for chromosome {chromosome}.")
            loco_model_df = self.__model_df.filter(
                ~f.col('header').rlike(f'^chr_{chromosome}_(alpha|block)'))
            self.set_model_df(loco_model_df)
            loco_y_hat_df = self.transform()
            loco_y_hat_df['contigName'] = chromosome
            y_hat_df = y_hat_df.append(loco_y_hat_df)

        self.set_model_df(orig_model_df)
        self.__y_hat_df = y_hat_df.set_index('contigName', append=True)
        return self.__y_hat_df

    def fit_transform(self) -> pd.DataFrame:
        """
        Fits a ridge regression model with a block matrix, then transforms the matrix using the model.

        Returns:
            Pandas DataFrame containing prediction y_hat values. The shape and order match labeldf such that the
            rows are indexed by sample ID and the columns by label. The column types are float64.
        """
        self.fit()
        return self.transform()

    def fit_transform_loco(self) -> pd.DataFrame:
        """
        Fits a ridge regression model with a block matrix, then transforms the matrix using the model.

        Returns:
            Pandas DataFrame containing prediction y_hat values. The shape and order match labeldf such that the
            rows are indexed by sample ID and the columns by label. The column types are float64.
        """
        self.fit()
        return self.transform_loco()
