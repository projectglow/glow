from .ridge_udfs import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.functions as f
from pyspark.sql.window import Window


class RidgeReducer:
    """
    The RidgeReducer class is intended to reduce the feature space of an N by M block matrix X to an N by P<<M block
    matrix.  This is done by fitting K ridge models within each block of X on one or more target labels, such that a
    block with L columns to begin with will be reduced to a block with K columns, where each column is the prediction
    of one ridge model for one target label.
    """
    def __init__(self, alphas):
        """
        RidgeReducer is initialized with a list of alpha values.

        Args:
            alphas : array_like of alpha values used in the ridge reduction
        """
        self.alphas = {f'alpha_{i}': a for i, a in enumerate(alphas)}

    def fit(self, blockdf, labeldf, indexdf):
        """
        Fits a ridge reducer model, represented by a Spark DataFrame containing coefficients for each of the ridge
        alpha parameters, for each block in the starting matrix, for each label in the target labels.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            indexdf : Spark DataFrame containing a mapping of sample_block ID to a list of corresponding sample IDs

        Returns:
            Spark DataFrame containing the model resulting from the fitting routine.
        """

        sample_index = {r.sample_block: r.sample_ids for r in indexdf.collect()}
        map_key_pattern = ['header_block', 'sample_block']
        reduce_key_pattern = ['header']

        if 'label' in blockdf.columns:
            map_key_pattern.append('label')
            reduce_key_pattern.append('label')

        map_udf = pandas_udf(
            lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, labeldf, sample_index),
            normal_eqn_struct, PandasUDFType.GROUPED_MAP)
        reduce_udf = pandas_udf(lambda key, pdf: reduce_normal_eqn(pdf), normal_eqn_struct,
                                PandasUDFType.GROUPED_MAP)
        model_udf = pandas_udf(
            lambda key, pdf: solve_normal_eqn(key, map_key_pattern, pdf, labeldf, self.alphas),
            model_struct, PandasUDFType.GROUPED_MAP)

        return blockdf \
            .groupBy(map_key_pattern) \
            .apply(map_udf) \
            .groupBy(reduce_key_pattern) \
            .apply(reduce_udf) \
            .groupBy(map_key_pattern) \
            .apply(model_udf)

    def transform(self, blockdf, labeldf, modeldf):
        """
        Transforms a starting block matrix to the reduced block matrix, using a reducer model produced by the
        RidgeReducer fit method.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            modeldf : Spark DataFrame produced by the RidgeReducer fit method, representing the reducer model

        Returns:
             Spark DataFrame representing the reduced block matrix
        """

        transform_key_pattern = ['header_block', 'sample_block']

        if 'label' in blockdf.columns:
            transform_key_pattern.append('label')

        transform_udf = pandas_udf(
            lambda key, pdf: apply_model(key, transform_key_pattern, pdf, labeldf, self.alphas),
            reduced_matrix_struct, PandasUDFType.GROUPED_MAP)

        return blockdf.join(modeldf.drop('sort_key'), ['header_block', 'sample_block', 'header']) \
            .groupBy(transform_key_pattern) \
            .apply(transform_udf)


class RidgeRegression:
    """
    The RidgeRegression class is used to fit ridge models against one or labels optimized over a provided list of
    ridge alpha parameters.  It is similar in function to RidgeReducer except that whereas RidgeReducer attempts to
    reduce a starting matrix X to a block matrix of smaller dimension, RidgeRegression is intended to find an optimal
    model of the form Y_hat ~ XB, where Y_hat is a matrix of one or more predicted labels and B is a matrix of
    coefficients.  The optimal ridge alpha value is chosen for each label by maximizing the average out of fold r2
    score.
    """
    def __init__(self, alphas):
        """
        RidgeRegression is initialized with a list of alpha values.

        Args:
            alphas : array_like of alpha values used in the ridge regression
        """
        self.alphas = {f'alpha_{i}': a for i, a in enumerate(alphas)}

    def fit(self, blockdf, labeldf, indexdf):
        """
        Fits a ridge regression model, represented by a Spark DataFrame containing coefficients for each of the ridge
        alpha parameters, for each block in the starting matrix, for each label in the target labels, as well as a
        Spark DataFrame containing the optimal ridge alpha value for each label.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            indexdf : Spark DataFrame containing a mapping of sample_block ID to a list of corresponding sample IDs

        Returns:
            Two Spark DataFrames, one containing the model resulting from the fitting routine and one containing the
            results of the cross validation procedure.
        """

        sample_index = {r.sample_block: r.sample_ids for r in indexdf.collect()}
        map_key_pattern = ['sample_block']
        reduce_key_pattern = ['header']

        if 'label' in blockdf.columns:
            map_key_pattern.append('label')
            reduce_key_pattern.append('label')

        map_udf = pandas_udf(
            lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, labeldf, sample_index),
            normal_eqn_struct, PandasUDFType.GROUPED_MAP)
        reduce_udf = pandas_udf(lambda key, pdf: reduce_normal_eqn(pdf), normal_eqn_struct,
                                PandasUDFType.GROUPED_MAP)
        model_udf = pandas_udf(
            lambda key, pdf: solve_normal_eqn(key, map_key_pattern, pdf, labeldf, self.alphas),
            model_struct, PandasUDFType.GROUPED_MAP)
        score_udf = pandas_udf(
            lambda key, pdf: score_models(key, map_key_pattern, pdf, labeldf, sample_index, self.
                                          alphas), cv_struct, PandasUDFType.GROUPED_MAP)

        modeldf = blockdf \
            .groupBy(map_key_pattern) \
            .apply(map_udf) \
            .groupBy(reduce_key_pattern) \
            .apply(reduce_udf) \
            .groupBy(map_key_pattern) \
            .apply(model_udf)

        cvdf = blockdf \
            .join(modeldf.drop('header_block', 'sort_key'), ['header', 'sample_block'], 'inner') \
            .groupBy(map_key_pattern) \
            .apply(score_udf) \
            .groupBy('label', 'alpha').agg(f.mean('r2').alias('r2_mean')) \
            .withColumn('modelRank', f.dense_rank().over(Window.partitionBy("label").orderBy(f.desc("r2_mean")))) \
            .filter('modelRank = 1') \
            .drop('modelRank')

        return modeldf, cvdf

    def transform(self, blockdf, labeldf, modeldf, cvdf):
        """
        Generates predictions for the target labels in the provided label DataFrame by applying the model resulting from
        the RidgeRegression fit method to the starting block matrix.

        Args:
            blockdf : Spark DataFrame representing the beginning block matrix X
            labeldf : Pandas DataFrame containing the target labels used in fitting the ridge models
            modeldf : Spark DataFrame produced by the RidgeRegression fit method, representing the reducer model
            cvdf : Spark DataFrame produced by the RidgeRegression fit method, containing the results of the cross
            validation routine.

        Returns:
            Spark DataFrame containing prediction y_hat values for each sample_block of samples for each label
        """

        transform_key_pattern = ['sample_block']

        if 'label' in blockdf.columns:
            transform_key_pattern.append('label')

        transform_udf = pandas_udf(
            lambda key, pdf: apply_model(key, transform_key_pattern, pdf, labeldf, self.alphas),
            reduced_matrix_struct, PandasUDFType.GROUPED_MAP)

        return blockdf.drop('header_block') \
            .join(modeldf.drop('header_block', 'sort_key'), ['sample_block', 'header']) \
            .groupBy(transform_key_pattern) \
            .apply(transform_udf) \
            .join(cvdf, ['label', 'alpha'], 'inner') \
            .select('sample_block', 'label', 'alpha', 'values')
