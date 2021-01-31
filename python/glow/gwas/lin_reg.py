import pandas as pd
import numpy as np
from nptyping import Float, NDArray
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import StringType, StructField
from scipy import stats
from typeguard import typechecked
from . import functions as gwas_fx
from .functions import _VALUES_COLUMN_NAME
from ..wgr.wgr_functions import _get_contigs_from_loco_df

__all__ = ['linear_regression']


@typechecked
def linear_regression(genotype_df: DataFrame,
                      phenotype_df: pd.DataFrame,
                      covariate_df: pd.DataFrame = pd.DataFrame({}),
                      offset_df: pd.DataFrame = pd.DataFrame({}),
                      contigs: Optional[List[str]] = None,
                      add_intercept: bool = True,
                      values_column: Union[str, Column] = 'values',
                      dt: type = np.float64) -> DataFrame:
    '''
    Uses linear regression to test for association between genotypes and one or more phenotypes.
    The implementation is a distributed version of the method used in regenie: 
    https://www.biorxiv.org/content/10.1101/2020.06.19.162354v2

    Implementation details:

    On the driver node, we decompose the covariate matrix into an orthonormal basis and use it to project the covariates 
    out of the phenotype matrix. The orthonormal basis and the phenotype residuals are broadcast as part of a Pandas UDF.
    In each Spark task, we project the covariates out of a block of genotypes and then compute the regression statistics for each phenotype,
    taking into account the distinct missingness patterns of each phenotype.

    Examples:
        >>> np.random.seed(42)
        >>> n_samples, n_phenotypes, n_covariates = (710, 3, 3)
        >>> phenotype_df = pd.DataFrame(np.random.random((n_samples, n_phenotypes)), columns=['p1', 'p2', 'p3'])
        >>> covariate_df = pd.DataFrame(np.random.random((n_samples, n_phenotypes)))
        >>> genotype_df = (spark.read.format('vcf').load('test-data/1kg_sample.vcf')
        ... .select('contigName', 'start', 'genotypes'))
        >>> results = glow.gwas.linear_regression(genotype_df, phenotype_df, covariate_df,
        ... values_column=glow.genotype_states('genotypes'))
        >>> results.head() # doctest: +ELLIPSIS
        Row(contigName='1', start=904164, effect=0.0453..., stderror=0.0214..., tvalue=2.114..., pvalue=0.0348..., phenotype='p1')

        >>> phenotype_df = pd.DataFrame(np.random.random((n_samples, n_phenotypes)), columns=['p1', 'p2', 'p3'])
        >>> covariate_df = pd.DataFrame(np.random.random((n_samples, n_phenotypes)))
        >>> genotype_df = (spark.read.format('vcf').load('test-data/1kg_sample.vcf')
        ... .select('contigName', 'start', 'genotypes'))
        >>> contigs = ['1', '2', '3']
        >>> offset_index = pd.MultiIndex.from_product([phenotype_df.index, contigs])
        >>> offset_df = pd.DataFrame(np.random.random((n_samples * len(contigs), n_phenotypes)),
        ... index=offset_index, columns=phenotype_df.columns)
        >>> results = glow.gwas.linear_regression(genotype_df, phenotype_df, covariate_df,
        ... offset_df=offset_df, values_column=glow.genotype_states('genotypes'))

    Args:
        genotype_df : Spark DataFrame containing genomic data
        phenotype_df : Pandas DataFrame containing phenotypic data
        covariate_df : An optional Pandas DataFrame containing covariates
        offset_df : An optional Pandas DataFrame containing the phenotype offset, as output by GloWGR's RidgeRegression
                    or Regenie step 1. The actual phenotype used for linear regression is the mean-centered,
                    residualized and scaled ``phenotype_df`` minus the appropriate offset. The ``offset_df`` may have
                    one or two levels of indexing.
                    If one level, the index should be the same as the ``phenotype_df``.
                    If two levels, the level 0 index should be the same as the ``phenotype_df``, and the level 1 index
                    should be the contig name. The two level index scheme allows for per-contig offsets like
                    LOCO predictions from GloWGR.
        contigs : When using LOCO offsets, this parameter indicates the contigs to analyze. You can use this parameter
                  to limit the size of the broadcasted data, which may be necessary with large sample sizes. If this
                  parameter is omitted, the contigs are inferred from the ``offset_df``.
        add_intercept : Whether or not to add an intercept column to the covariate DataFrame
        values_column : A column name or column expression to test with linear regression. If a column name is provided,
                        ``genotype_df`` should have a column with this name and a numeric array type. If a column
                        expression is provided, the expression should return a numeric array type.
        dt : The numpy datatype to use in the linear regression test. Must be ``np.float32`` or ``np.float64``.

    Returns:
        A Spark DataFrame that contains

        - All columns from ``genotype_df`` except the ``values_column`` and the ``genotypes`` column if one exists
        - ``effect``: The effect size estimate for the genotype
        - ``stderror``: The estimated standard error of the effect
        - ``tvalue``: The T statistic
        - ``pvalue``: P value estimated from a two sided T-test
        - ``phenotype``: The phenotype name as determined by the column names of ``phenotype_df``
    '''

    gwas_fx._check_spark_version(genotype_df.sql_ctx.sparkSession)

    gwas_fx._validate_covariates_and_phenotypes(covariate_df, phenotype_df, is_binary=False)

    sql_type = gwas_fx._regression_sql_type(dt)

    genotype_df = gwas_fx._prepare_genotype_df(genotype_df, values_column, sql_type)

    # Construct output schema
    result_fields = [
        StructField('effect', sql_type),
        StructField('stderror', sql_type),
        StructField('tvalue', sql_type),
        StructField('pvalue', sql_type),
        StructField('phenotype', StringType())
    ]
    result_struct = gwas_fx._output_schema(genotype_df.schema.fields, result_fields)

    C = covariate_df.to_numpy(dt, copy=True)
    if add_intercept:
        C = gwas_fx._add_intercept(C, phenotype_df.shape[0])

    # Prepare covariate basis and phenotype residuals
    Q = np.linalg.qr(C)[0]
    Y = phenotype_df.to_numpy(dt, copy=True)
    Y_mask = (~np.isnan(Y)).astype(dt)
    Y = np.nan_to_num(Y, copy=False)
    Y -= Y.mean(axis=0)  # Mean-center
    Y = gwas_fx._residualize_in_place(Y, Q) * Y_mask  # Residualize
    Y_scale = np.sqrt(np.sum(Y**2, axis=0) / (Y_mask.sum(axis=0) - Q.shape[1]))
    Y /= Y_scale[None, :]  # Scale

    Y_state = _create_YState(Y, phenotype_df, offset_df, Y_mask, dt, contigs)

    dof = C.shape[0] - C.shape[1] - 1

    def map_func(pdf_iterator):
        for pdf in pdf_iterator:
            yield gwas_fx._loco_dispatch(pdf, Y_state, _linear_regression_inner, Y_mask, Y_scale, Q,
                                         dof,
                                         phenotype_df.columns.to_series().astype('str'))

    return genotype_df.mapInPandas(map_func, result_struct)


@dataclass
class YState:
    '''
    Keeps track of state that varies per contig
    '''
    Y: NDArray[(Any, Any), Float]
    YdotY: NDArray[(Any), Float]


def _create_YState(Y: NDArray[(Any, Any), Float], phenotype_df: pd.DataFrame,
                   offset_df: pd.DataFrame, Y_mask: NDArray[(Any, Any), Float], dt,
                   contigs: Optional[List[str]]) -> Union[YState, Dict[str, YState]]:

    offset_type = gwas_fx._validate_offset(phenotype_df, offset_df)
    if offset_type != gwas_fx._OffsetType.LOCO_OFFSET:
        return _create_one_YState(Y, phenotype_df, offset_df, Y_mask, dt)

    if contigs is None:
        contigs = _get_contigs_from_loco_df(offset_df)
    return {
        contig: _create_one_YState(Y, phenotype_df, offset_df.xs(contig, level=1), Y_mask, dt)
        for contig in contigs
    }


def _create_one_YState(Y: NDArray[(Any, Any), Float], phenotype_df: pd.DataFrame,
                       offset_df: pd.DataFrame, Y_mask: NDArray[(Any, Any), Float], dt) -> YState:
    if not offset_df.empty:
        base_Y = pd.DataFrame(Y, phenotype_df.index, phenotype_df.columns)
        # Reindex so the numpy array maintains ordering after subtracting offset
        Y = (base_Y - offset_df).reindex(phenotype_df.index).to_numpy(dt)
    Y *= Y_mask
    return YState(Y, np.sum(Y * Y, axis=0))


@typechecked
def _linear_regression_inner(genotype_pdf: pd.DataFrame, Y_state: YState,
                             Y_mask: NDArray[(Any, Any), Float], Y_scale: NDArray[(Any, ), Float],
                             Q: NDArray[(Any, Any), Float], dof: int,
                             phenotype_names: pd.Series) -> pd.DataFrame:
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

    X = np.column_stack(genotype_pdf[_VALUES_COLUMN_NAME].array)
    X = gwas_fx._residualize_in_place(X, Q)

    XdotY = Y_state.Y.T @ X
    XdotX_reciprocal = 1 / gwas_fx._einsum('sp,sg,sg->pg', Y_mask, X, X)
    betas = XdotY * XdotX_reciprocal
    standard_error = np.sqrt((Y_state.YdotY[:, None] * XdotX_reciprocal - betas * betas) / dof)
    T = betas / standard_error
    pvalues = 2 * stats.distributions.t.sf(np.abs(T), dof)

    del genotype_pdf[_VALUES_COLUMN_NAME]
    num_genotypes = genotype_pdf.shape[0]
    out_df = pd.concat([genotype_pdf] * Y_state.Y.shape[1])
    Y_scale_mat = Y_scale[:, None]
    out_df['effect'] = list(np.ravel(betas * Y_scale_mat))
    out_df['stderror'] = list(np.ravel(standard_error * Y_scale_mat))
    out_df['tvalue'] = list(np.ravel(T))
    out_df['pvalue'] = list(np.ravel(pvalues))
    out_df['phenotype'] = phenotype_names.repeat(num_genotypes).tolist()

    return out_df
