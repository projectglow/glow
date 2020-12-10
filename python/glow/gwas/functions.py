from pyspark.sql import SparkSession, functions as fx
from pyspark.sql.types import StructField, StructType, FloatType, DoubleType, ArrayType
from typing import Any, List, Set
import numpy as np
import pandas as pd
from nptyping import Float, NDArray
from typeguard import typechecked
import opt_einsum as oe
from ..wgr.linear_model.functions import __assert_all_present, __check_binary

_VALUES_COLUMN_NAME = '_glow_regression_values'
_GENOTYPES_COLUMN_NAME = 'genotypes'

def _check_spark_version(spark: SparkSession) -> bool:
    if int(spark.version.split('.')[0]) < 3:
        raise AttributeError('Pandas based regression tests are only supported on Spark 3.0 or greater')


def _output_schema(input_fields: List[StructField], result_fields: List[StructField]) -> StructType:

    fields = [field for field in input_fields if field.name != _VALUES_COLUMN_NAME] + result_fields
    return StructType(fields)

def _validate_covariates_and_phenotypes(covariate_df, phenotype_df, is_binary):
    for col in covariate_df:
        __assert_all_present(covariate_df, col, 'covariate')
    if not covariate_df.empty:
        if phenotype_df.shape[0] != covariate_df.shape[0]:
            raise ValueError(
                f'phenotype_df and covariate_df must have the same number of rows ({phenotype_df.shape[0]} != {covariate_df.shape[0]}'
            )
    if is_binary:
        __check_binary(phenotype_df)

def _regression_sql_type(dt):
    if dt == np.float32:
        return FloatType()
    elif dt == np.float64:
        return DoubleType()
    else:
        raise ValueError('dt must be np.float32 or np.float64')

def _prepare_genotype_df(genotype_df, values_column, sql_type):
    if isinstance(values_column, str):
        if values_column == _GENOTYPES_COLUMN_NAME:
            raise ValueError(f'The values column should not be called "{_GENOTYPES_COLUMN_NAME}"')
        out = (genotype_df.withColumn(_VALUES_COLUMN_NAME,
                                              fx.col(values_column).cast(
                                                  ArrayType(sql_type))).drop(values_column))
    else:
        out = genotype_df.withColumn(_VALUES_COLUMN_NAME,
                                             values_column.cast(ArrayType(sql_type)))

    if _GENOTYPES_COLUMN_NAME in [field.name for field in genotype_df.schema]:
        out = genotype_df.drop(_GENOTYPES_COLUMN_NAME)
    return out


@typechecked
def _add_intercept(C: NDArray[(Any, Any), Float], num_samples: int) -> NDArray[(Any, Any), Float]:
    intercept = np.ones((num_samples, 1))
    return np.hstack((intercept, C)) if C.size else intercept

@typechecked
def _einsum(subscripts: str, *operands: NDArray) -> NDArray:
    '''
    A wrapper around np.einsum to ensure uniform options.
    '''
    return oe.contract(subscripts, *operands, casting='no', optimize='dp')

@typechecked
def _add_intercept(C: NDArray[(Any, Any), Float], num_samples: int) -> NDArray[(Any, Any), Float]:
    intercept = np.ones((num_samples, 1))
    return np.hstack((intercept, C)) if C.size else intercept

def _have_same_elements(idx1: pd.Index, idx2: pd.Index) -> bool:
    return idx1.sort_values().equals(idx2.sort_values())
