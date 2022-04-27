import pandas as pd
import numpy as np
from nptyping import Float, NDArray, Int32
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import StringType, StructField, IntegerType
from scipy import stats
from typeguard import typechecked
from . import functions as gwas_fx
from .functions import _VALUES_COLUMN_NAME, _get_indices_to_drop
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
                      dt: type = np.float64,
                      verbose_output: bool = False,
                      intersect_samples: bool = False,
                      genotype_sample_ids: Optional[List[str]] = None) -> DataFrame:
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
        verbose_output: Whether or not to generate additional test statistics (n, sum_x, y_transpose_x)
                        to the output DataFrame.  These values are derived directly from phenotype_df and genotype_df,
                        and does not reflect any standardization performed as part of the implementation of
                        linear_regression.
        intersect_samples: The current implementation of linear regression is optimized for speed,
                           but is not robust to high levels missing phenotype values.  Without handling missingness
                           appropriately, pvalues may become inflated due to imputation.  When intersect_samples is
                           enabled, samples that do no exist in the phenotype dataframe will be dropped from
                           genotypes, offsets, and covariates prior to regression analysis.  Note that if phenotypes in
                           phenotypes_df contain missing values, these samples will not be automatically dropped.
                           The user is responsible for determining their desired levels of missingness and imputation.
                           Drop any rows with missing values from phenotype_df prior to linear_regression to prevent
                           any imputation.  If covariates are provided, covariate and phenotype samples will
                           automatically be intersected.
        genotype_sample_ids: Sample ids from genotype_df.
                             i.e. from applying glow.wgr.functions.get_sample_ids(genotype_df) or
                             if include_sample_ids=False was used during the generation genotype_df, then using an
                             externally managed list of sample_ids that correspond to the array of genotype calls.

    Returns:
        A Spark DataFrame that contains

        - All columns from ``genotype_df`` except the ``values_column`` and the ``genotypes`` column if one exists
        - ``effect``: The effect size estimate for the genotype
        - ``stderror``: The estimated standard error of the effect
        - ``tvalue``: The T statistic
        - ``pvalue``: P value estimated from a two sided T-test
        - ``phenotype``: The phenotype name as determined by the column names of ``phenotype_df``
        - ``n``(int): (verbose_output only) number of samples with non-null phenotype
        - ``sum_x``(float): (verbose_output only) sum of genotype inputs
        - ``y_transpose_x``(float): (verbose_output only) dot product of phenotype response (missing values encoded as zeros)
                             and genotype input, i.e. phenotype value * number of alternate alleles
    '''

    gwas_fx._check_spark_version(genotype_df.sql_ctx.sparkSession)

    gwas_fx._validate_covariates_and_phenotypes(covariate_df,
                                                phenotype_df,
                                                is_binary=False,
                                                intersect_samples=intersect_samples)

    sql_type = gwas_fx._regression_sql_type(dt)

    genotype_df = gwas_fx._prepare_genotype_df(genotype_df, values_column, sql_type)

    gt_indices_to_drop = None
    _covs = covariate_df
    if intersect_samples:
        if not genotype_sample_ids:
            raise ValueError("genotype_sample_ids required when intersect_samples enabled")
        _covs = covariate_df.loc[phenotype_df.index, :]
        gt_indices_to_drop = _get_indices_to_drop(phenotype_df, genotype_sample_ids)
        if not offset_df.empty:
            if offset_df.index.nlevels == 1:  # Indexed by sample id
                offset_df = offset_df.reindex(phenotype_df.index)
            elif offset_df.index.nlevels == 2:  # Indexed by sample id and contig
                offset_df = offset_df[offset_df.index.get_level_values(0).isin(phenotype_df.index)]

    C = _covs.to_numpy(dt, copy=True)
    if add_intercept:
        C = gwas_fx._add_intercept(C, phenotype_df.shape[0])

    # Prepare covariate basis and phenotype residuals
    Q = np.linalg.qr(C)[0]
    Y = phenotype_df.to_numpy(dt, copy=True)
    Y_mask = (~np.isnan(Y)).astype(dt)
    Y = np.nan_to_num(Y, copy=False)
    Y_for_verbose_output = np.copy(Y) if verbose_output else None
    Y -= Y.mean(axis=0)  # Mean-center
    Y = gwas_fx._residualize_in_place(Y, Q) * Y_mask  # Residualize
    Y_scale = np.sqrt(np.sum(Y**2, axis=0) / (Y_mask.sum(axis=0) - Q.shape[1]))
    Y /= Y_scale[None, :]  # Scale

    Y_state = _create_YState(Y, phenotype_df, offset_df, Y_mask, dt, contigs)

    dof = C.shape[0] - C.shape[1] - 1

    return _generate_linreg_output(genotype_df, sql_type, Y_state, Y_mask, Y_scale, Q, dof,
                                   phenotype_df, Y_for_verbose_output, verbose_output,
                                   gt_indices_to_drop)


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


# @typechecked -- typeguard does not support numpy array
def _linear_regression_inner(genotype_pdf: pd.DataFrame, Y_state: YState,
                             Y_mask: NDArray[(Any, Any), Float], Y_scale: NDArray[(Any, ), Float],
                             Q: NDArray[(Any, Any), Float], dof: int, phenotype_names: pd.Series,
                             Y_for_verbose_output: Optional[NDArray[(Any, Any), Float]],
                             verbose_output: Optional[bool],
                             gt_indices_to_drop: Optional[NDArray[(Any, ), Int32]]) -> pd.DataFrame:
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

    genotype_values = genotype_pdf[_VALUES_COLUMN_NAME].array
    X = np.column_stack(genotype_values)
    if gt_indices_to_drop is not None and gt_indices_to_drop.size:
        X = np.delete(X, gt_indices_to_drop, axis=0)

    genotype_pdf.drop(_VALUES_COLUMN_NAME, axis=1, inplace=True)
    num_genotypes = genotype_pdf.shape[0]
    out_df = pd.concat([genotype_pdf] * Y_state.Y.shape[1])
    if verbose_output:
        out_df["n"] = list(np.ravel(Y_mask.T @ np.ones(X.shape)))
        out_df["sum_x"] = list(np.ravel(Y_mask.T @ X))
        out_df["y_transpose_x"] = list(np.ravel(Y_for_verbose_output.T @ X))
    X = gwas_fx._residualize_in_place(X, Q)

    XdotY = Y_state.Y.T @ X
    XdotX_reciprocal = 1 / gwas_fx._einsum('sp,sg,sg->pg', Y_mask, X, X)
    betas = XdotY * XdotX_reciprocal
    standard_error = np.sqrt((Y_state.YdotY[:, None] * XdotX_reciprocal - betas * betas) / dof)
    T = betas / standard_error
    pvalues = 2 * stats.distributions.t.sf(np.abs(T), dof)

    Y_scale_mat = Y_scale[:, None]
    out_df['effect'] = list(np.ravel(betas * Y_scale_mat))
    out_df['stderror'] = list(np.ravel(standard_error * Y_scale_mat))
    out_df['tvalue'] = list(np.ravel(T))
    out_df['pvalue'] = list(np.ravel(pvalues))
    out_df['phenotype'] = phenotype_names.repeat(num_genotypes).tolist()

    return out_df


def _generate_linreg_output(genotype_df, sql_type, Y_state, Y_mask, Y_scale, Q, dof, phenotype_df,
                            Y_for_verbose_output, verbose_output, gt_indices_to_drop) -> DataFrame:
    # Construct output schema
    result_fields = [
        StructField('effect', sql_type),
        StructField('stderror', sql_type),
        StructField('tvalue', sql_type),
        StructField('pvalue', sql_type),
        StructField('phenotype', StringType())
    ]

    if verbose_output:
        result_fields += ([
            StructField('n', IntegerType()),
            StructField('sum_x', sql_type),
            StructField('y_transpose_x', sql_type)
        ])

    result_struct = gwas_fx._output_schema(genotype_df.schema.fields, result_fields)

    def map_func(pdf_iterator):
        for pdf in pdf_iterator:
            yield gwas_fx._loco_dispatch(pdf, Y_state, _linear_regression_inner, Y_mask, Y_scale, Q,
                                         dof,
                                         phenotype_df.columns.to_series().astype('str'),
                                         Y_for_verbose_output, verbose_output, gt_indices_to_drop)

    return genotype_df.mapInPandas(map_func, result_struct)
