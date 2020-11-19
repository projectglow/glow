import pandas as pd
import numpy as np
from nptyping import NDArray
from dataclasses import dataclass
from typing import Any, Dict, List, Union
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from scipy import stats
from typeguard import typechecked
from ..wgr.linear_model.functions import __assert_all_present

__all__ = ['linear_regression']


@typechecked
def linear_regression(genotype_df: DataFrame,
                      phenotype_df: pd.DataFrame,
                      covariate_df: pd.DataFrame = pd.DataFrame({}),
                      offset_df: pd.DataFrame = pd.DataFrame({}),
                      fit_intercept: bool = True,
                      values_column: str = 'values') -> DataFrame:
    '''
    Uses linear regression to test for association between genotypes and one or more phenotypes.
    The implementation is based on regenie: https://www.biorxiv.org/content/10.1101/2020.06.19.162354v2

    On the driver node, we decompose the covariate matrix into an orthonormal basis and use it to project the covariates 
    out of the phenotype matrix. The orthonormal basis and the phenotype residuals are broadcast as part of a Pandas UDF.
    In each Spark task, we project the covariates out of a block of genotypes and then compute the regression statistics for each phenotype,
    taking into account the distinct missingness patterns of each phenotype.

    Args:
        genotype_df : Spark DataFrame containing genomic data
        phenotype_df : Pandas DataFrame containing phenotypic data
        covariate_df : An optional Pandas DataFrame containing covariates
        fit_intercept : Whether or not to add an intercept column to the covariate DataFrame
        values_column : The name of the column in `genotype_df` that contains the values to be tested with linear regression

    Returns:
        A Spark DataFrame that contains:
        - All columns from `genotype_df` except the `values_column`
        - `effect`: The effect size estimate for the genotype
        - `stderror`: The estimated standard error of the effect
        - `tvalue`: The T statistic
        - `pvalue`: P value estimed from a two sided t-test
        - `phenotype`: The phenotype name as determined by the column names of `phenotype_df`
    '''
    if int(genotype_df.sql_ctx.sparkSession.version.split('.')[0]) < 3:
        raise AttributeError(
            'Pandas based linear regression is only supported on Spark 3.0 or greater')

    # Validate input
    for col in covariate_df:
        __assert_all_present(covariate_df, col, 'covariate')
    if not covariate_df.empty:
        if phenotype_df.shape[0] != covariate_df.shape[0]:
            raise ValueError(
                f'phenotype_df and covariate_df must have the same number of rows ({phenotype_df.shape[0]} != {covariate_df.shape[0]}'
            )



    # Construct output schema
    result_fields = [
        StructField('effect', DoubleType()),
        StructField('stderror', DoubleType()),
        StructField('tvalue', DoubleType()),
        StructField('pvalue', DoubleType()),
        StructField('phenotype', StringType())
    ]
    fields = [field
              for field in genotype_df.schema.fields if field.name != values_column] + result_fields
    result_struct = StructType(fields)

    C = covariate_df.to_numpy(np.float64, copy=True)
    if fit_intercept:
        C = _add_intercept(C, phenotype_df.shape[0])

    # Prepare covariate basis and phenotype residuals
    Q = np.linalg.qr(C)[0]
    Y = phenotype_df.to_numpy(np.float64, copy=True)
    Y_mask = (~np.isnan(Y)).astype(np.float64)
    np.nan_to_num(Y, copy=False)
    _residualize_in_place(Y, Q)
    
    if not offset_df.empty:
        if offset_df.index.nlevels == 1: # Indexed by sample id
            Y_state = _create_YState_from_offset(Y, Y_mask, phenotype_df, offset_df)
        elif offset_df.index.nlevels == 2: # Indexed by sample id and contig
            all_contigs = offset_df.index.get_level_values(1).unique()
            Y_state = {contig: _create_YState_from_offset(Y, Y_mask, phenotype_df, offset_df.xs(contig, level=1)) for contig in all_contigs}
    else:
        Y_state = _create_YState(Y, Y_mask)

    dof = C.shape[0] - C.shape[1] - 1

    def map_func(pdf_iterator):
        for pdf in pdf_iterator:
            yield _linear_regression_partition(pdf, Y_state, Y_mask, Q, dof,
                                           phenotype_df.columns.to_series().astype('str'),
                                           values_column)

    return genotype_df.mapInPandas(map_func, result_struct)

def _create_YState_from_offset(
    Y: NDArray[(Any, Any), np.float64], 
    Y_mask: NDArray[(Any, Any), np.float64],
    phenotype_df: pd.DataFrame, 
    offset_df: pd.DataFrame) -> NDArray[(Any, Any), np.float64]:
    Y = (pd.DataFrame(Y, phenotype_df.index, phenotype_df.columns) - offset_df).to_numpy(np.float64)
    return _create_YState(Y, Y_mask)

def _create_YState(Y: NDArray[(Any, Any), np.float64], Y_mask: NDArray[(Any, Any), np.float64]) -> NDArray[(Any, Any), np.float64]:
    Y *= Y_mask
    return YState(Y, np.sum(Y * Y, axis=0))
    


@dataclass
class YState:
    '''
    Keeps track of state that varies per contig
    '''
    Y: NDArray[(Any, Any), np.float64]
    YdotY: NDArray[(Any), np.float64]

@typechecked
def _add_intercept(C: NDArray[(Any, Any), np.float64], num_samples: int):
    intercept = np.ones((num_samples, 1))
    return np.hstack((intercept, C)) if C.size else intercept


@typechecked
def _einsum(subscripts: str, *operands: NDArray):
    '''
    A wrapper around np.einsum to ensure uniform options.
    '''
    return np.einsum(subscripts, *operands, casting='no')


@typechecked
def _residualize_in_place(M: NDArray[(Any, Any), np.float64],
                          Q: NDArray[(Any, Any), np.float64]) -> NDArray[(Any, Any), np.float64]:
    '''
    Residualize a matrix in place using an orthonormal basis. The residualized matrix
    is returned for easy chaining.
    '''
    M -= Q @ (Q.T @ M)
    return M

def _linear_regression_partition(genotype_pdf: pd.DataFrame, Y_state: Union[YState, Dict[str, YState]], *args):
    if isinstance(Y_state, dict):
        return genotype_pdf.groupby('contigName', sort=False, as_index=False)\
            .apply(lambda pdf: _linear_regression_inner(pdf, Y_state[pdf['contigName'].iloc[0]], *args))
    else:
        return _linear_regression_inner(genotype_pdf, Y_state, *args)

@typechecked
def _linear_regression_inner(genotype_pdf: pd.DataFrame, Y_state: YState, Y_mask: NDArray[(Any, Any),
                                                                                np.float64],
                             Q: NDArray[(Any, Any), np.float64], dof: int,
                             phenotype_names: pd.Series, values_column: str) -> pd.DataFrame:
    '''
    Applies a linear regression model to a block of genotypes. We first project the covariates out of the
    genotype block and then perform single variate linear regression for each site.

    To account for samples with missing traits, we additionally accept a mask indicating which samples are missing
    for each phenotype. This mask is used to ensure that missing samples are not included when summing across individuals.

    Rather than use traditional matrix indices in the einsum expressions, we use semantic indices.
    s: sample
    g: genotype
    p: phenotype

    So, if a matrix's indices are `sg` (like the X matrix), it has one row per sample and one column per genotype.
    '''
    X = _residualize_in_place(np.column_stack(genotype_pdf[values_column].array), Q)
    XdotY = Y_state.Y.T @ X
    XdotX_reciprocal = 1 / _einsum('sp,sg,sg->pg', Y_mask, X, X)
    betas = XdotY * XdotX_reciprocal
    standard_error = np.sqrt((Y_state.YdotY[:, None] * XdotX_reciprocal - betas * betas) / dof)
    T = betas / standard_error
    pvalues = 2 * stats.distributions.t.sf(np.abs(T), dof)

    del genotype_pdf[values_column]
    out_df = pd.concat([genotype_pdf] * Y_state.Y.shape[1])
    out_df['effect'] = list(np.ravel(betas))
    out_df['stderror'] = list(np.ravel(standard_error))
    out_df['tvalue'] = list(np.ravel(T))
    out_df['pvalue'] = list(np.ravel(pvalues))
    out_df['phenotype'] = phenotype_names.repeat(genotype_pdf.shape[0]).tolist()

    return out_df
