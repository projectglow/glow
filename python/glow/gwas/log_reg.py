from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, DataType, StructType
from typing import Any, List, Optional, Dict, Union
import pandas as pd
import numpy as np
from pyspark.sql import DataFrame, SparkSession
import statsmodels.api as sm
from dataclasses import dataclass
from typeguard import typechecked
from nptyping import Float, NDArray
from scipy import stats
import opt_einsum as oe
from . import functions as gwas_fx
from . import approx_firth as af
from .functions import _VALUES_COLUMN_NAME
from ..wgr.wgr_functions import reshape_for_gwas

__all__ = ['logistic_regression']

correction_none = 'none'
correction_approx_firth = 'approx-firth'


@typechecked
def logistic_regression(genotype_df: DataFrame,
                        phenotype_df: pd.DataFrame,
                        covariate_df: pd.DataFrame = pd.DataFrame({}),
                        offset_df: pd.DataFrame = pd.DataFrame({}),
                        correction: str = correction_approx_firth,
                        pvalue_threshold: float = 0.05,
                        contigs: Optional[List[str]] = None,
                        add_intercept: bool = True,
                        values_column: str = 'values',
                        dt: type = np.float64) -> DataFrame:
    '''
    Uses logistic regression to test for association between genotypes and one or more binary
    phenotypes. This is a distributed version of the method from regenie:
    https://www.biorxiv.org/content/10.1101/2020.06.19.162354v2

    Implementation details:

    On the driver node, we fit a logistic regression model based on the covariates for each
    phenotype:

    .. math::
        logit(y) \sim C

    where :math:`y` is a phenotype vector and :math:`C` is the covariate matrix.

    We compute the probability predictions :math:`\hat{y}` and broadcast the residuals (:math:`y - \hat{y}`),
    :math:`\gamma` vectors (where :math:`\gamma = \hat{y} * (1 - \hat{y})`), and
    :math:`(C^\intercal \gamma C)^{-1}` matrices. In each task, we then adjust the new genotypes based on the null fit,
    perform a score test as a fast scan for potentially significant variants, and then test variants with p-values below
    a threshold using a more selective, more expensive test.

    Args:
        genotype_df : Spark DataFrame containing genomic data
        phenotype_df : Pandas DataFrame containing phenotypic data
        covariate_df : An optional Pandas DataFrame containing covariates
        offset_df : An optional Pandas DataFrame containing the phenotype offset. This value will be used
                    as an offset in the covariate only and per variant logistic regression models. The ``offset_df`` may
                    have one or two levels of indexing. If one level, the index should be the same as the ``phenotype_df``.
                    If two levels, the level 0 index should be the same as the ``phenotype_df``, and the level 1 index
                    should be the contig name. The two level index scheme allows for per-contig offsets like
                    LOCO predictions from GloWGR.
        correction : Which test to use for variants that meet a significance threshold for the score test. Supported
                     methods are ``none`` and ``approx-firth``.
        pvalue_threshold : Variants with a pvalue below this threshold will be tested using the ``correction`` method.
        contigs : When using LOCO offsets, this parameter indicates the contigs to analyze. You can use this parameter to limit the size of the broadcasted data, which may
                  be necessary with large sample sizes. If this parameter is omitted, the contigs are inferred from
                  the ``offset_df``.
        add_intercept : Whether or not to add an intercept column to the covariate DataFrame
        values_column : A column name or column expression to test with linear regression. If a column name is provided,
                        ``genotype_df`` should have a column with this name and a numeric array type. If a column expression
                        is provided, the expression should return a numeric array type.
        dt : The numpy datatype to use in the linear regression test. Must be ``np.float32`` or ``np.float64``.

    Returns:
        A Spark DataFrame that contains

        - All columns from ``genotype_df`` except the ``values_column`` and the ``genotypes`` column if one exists
        - ``effect``: The effect size (if approximate Firth correction was applied)
        - ``stderror``: Standard error of the effect size (if approximate Firth correction was applied)
        - ``correctionSucceeded``: Whether the correction succeeded (if the correction test method is not ``none``).
          ``True`` if succeeded, ``False`` if failed, ``null`` if correction was not applied.
        - ``chisq``: The chi squared test statistic according to the score test or the correction method
        - ``pvalue``: p-value estimated from the test statistic
        - ``phenotype``: The phenotype name as determined by the column names of ``phenotype_df``
    '''

    spark = genotype_df.sql_ctx.sparkSession
    gwas_fx._check_spark_version(spark)
    gwas_fx._validate_covariates_and_phenotypes(covariate_df, phenotype_df, is_binary=True)
    sql_type = gwas_fx._regression_sql_type(dt)
    genotype_df = gwas_fx._prepare_genotype_df(genotype_df, values_column, sql_type)
    base_result_fields = [
        StructField('chisq', sql_type),
        StructField('pvalue', sql_type),
        StructField('phenotype', StringType())
    ]
    if correction == correction_approx_firth:
        result_fields = [
            StructField('effect', sql_type),
            StructField('stderror', sql_type),
            StructField('correctionSucceeded', BooleanType())
        ] + base_result_fields
    elif correction == correction_none:
        result_fields = base_result_fields
    else:
        raise ValueError(
            f"Only supported correction methods are '{correction_none}' and '{correction_approx_firth}'"
        )

    result_struct = gwas_fx._output_schema(genotype_df.schema.fields, result_fields)
    C = covariate_df.to_numpy(dt, copy=True)
    if add_intercept:
        C = gwas_fx._add_intercept(C, phenotype_df.shape[0])
    Y = phenotype_df.to_numpy(dt, copy=True)
    Y_mask = ~(np.isnan(Y))
    np.nan_to_num(Y, copy=False)

    if correction == correction_approx_firth:
        Q = np.linalg.qr(C)[0]
    else:
        Q = None

    state = _create_log_reg_state(spark, phenotype_df, offset_df, sql_type, C, correction,
                                  add_intercept, contigs)

    phenotype_names = phenotype_df.columns.to_series().astype('str')

    def map_func(pdf_iterator):
        for pdf in pdf_iterator:
            yield gwas_fx._loco_dispatch(pdf, state, _logistic_regression_inner, C, Y, Y_mask, Q,
                                         correction, pvalue_threshold, phenotype_names)

    return genotype_df.mapInPandas(map_func, result_struct)


@dataclass
class LogRegState:
    inv_CtGammaC: NDArray[(Any, Any, Any), Float]  # n_phenotypes x n_covariates x n_covariates
    gamma: NDArray[(Any, Any), Float]  # n_samples x n_phenotypes
    Y_res: NDArray[(Any, Any), Float]  # n_samples x n_phenotypes
    firth_offset: Optional[NDArray[(Any, Any), Float]]  # n_samples x n_phenotypes


def _logistic_null_model_predictions(y, X, mask, offset):
    if offset is not None:
        offset = offset[mask]
    model = sm.GLM(y[mask],
                   X[mask, :],
                   family=sm.families.Binomial(),
                   offset=offset,
                   missing='ignore')
    fit_result = model.fit()
    predictions = model.predict(fit_result.params)

    # Store 0 as prediction for samples with missing phenotypes
    remapped_predictions = np.zeros(y.shape)
    remapped_predictions[mask] = predictions
    return remapped_predictions


def _prepare_one_phenotype(C: NDArray[(Any, Any), Float], row: pd.Series, correction: str,
                           includes_intercept: bool) -> pd.Series:
    '''
    Creates the broadcasted information for one (phenotype, offset) pair. The returned series
    contains the information eventually stored in a LogRegState.

    This function accepts and returns a pandas series for integration with Pandas UDFs and
    pd.DataFrame.apply.
    '''
    y = row['values']
    mask = ~np.isnan(y)
    offset = row.get('offset')
    y_pred = _logistic_null_model_predictions(y, C, mask, offset)
    y_res = np.nan_to_num(y - y_pred)
    gamma = y_pred * (1 - y_pred)
    CtGammaC = C.T @ (gamma[:, None] * C)
    inv_CtGammaC = np.linalg.inv(CtGammaC)
    row.label = str(row.label)  # Ensure that the phenotype name is a string
    row.drop(['values', 'offset'], inplace=True, errors='ignore')
    row['y_res'], row['gamma'], row['inv_CtGammaC'] = np.ravel(y_res), np.ravel(gamma), np.ravel(
        inv_CtGammaC)
    if correction == correction_approx_firth:
        row['firth_offset'] = np.ravel(
            af.perform_null_firth_fit(y, C, mask, offset, includes_intercept))
    return row


@typechecked
def _pdf_to_log_reg_state(pdf: pd.DataFrame, phenotypes: pd.Series, n_covar: int) -> LogRegState:
    '''
    Converts a Pandas DataFrame with the contents of a LogRegState object
    into a more convenient form.
    '''
    # Ensure that the null fit state is sorted identically to the input phenotype_df
    sorted_pdf = pdf.set_index('label').reindex(phenotypes, axis='rows')
    inv_CtGammaC = np.row_stack(sorted_pdf['inv_CtGammaC'].array).reshape(
        phenotypes.size, n_covar, n_covar)
    gamma = np.column_stack(sorted_pdf['gamma'].array)
    Y_res = np.column_stack(sorted_pdf['y_res'].array)
    if 'firth_offset' in sorted_pdf:
        firth_offset = np.column_stack(sorted_pdf['firth_offset'].array)
    else:
        firth_offset = None
    return LogRegState(inv_CtGammaC, gamma, Y_res, firth_offset)


@typechecked
def _create_log_reg_state(
        spark: SparkSession, phenotype_df: pd.DataFrame, offset_df: pd.DataFrame,
        sql_type: DataType, C: NDArray[(Any, Any), Float], correction: str, add_intercept: bool,
        contigs: Optional[List[str]]) -> Union[LogRegState, Dict[str, LogRegState]]:
    '''
    Creates the broadcasted LogRegState object (or one object per contig if LOCO offsets were provided).

    Fitting the null logistic models can be expensive, so the work is distributed across the cluster
    using Pandas UDFs.
    '''
    offset_type = gwas_fx._validate_offset(phenotype_df, offset_df)
    if offset_type == gwas_fx._OffsetType.LOCO_OFFSET and contigs is not None:
        offset_df = offset_df.loc[pd.IndexSlice[:, contigs], :]
    pivoted_phenotype_df = reshape_for_gwas(spark, phenotype_df)
    result_fields = [
        StructField('label', StringType()),
        StructField('y_res', ArrayType(sql_type)),
        StructField('gamma', ArrayType(sql_type)),
        StructField('inv_CtGammaC', ArrayType(sql_type))
    ]

    if correction == correction_approx_firth:
        result_fields.append(StructField('firth_offset', ArrayType(sql_type)))

    if offset_type == gwas_fx._OffsetType.NO_OFFSET:
        df = pivoted_phenotype_df
    else:
        pivoted_offset_df = reshape_for_gwas(spark, offset_df).withColumnRenamed('values', 'offset')
        df = pivoted_offset_df.join(pivoted_phenotype_df, on='label')

    if offset_type == gwas_fx._OffsetType.LOCO_OFFSET:
        result_fields.append(StructField('contigName', StringType()))

    def map_func(pdf_iterator):
        for pdf in pdf_iterator:
            yield pdf.apply(lambda r: _prepare_one_phenotype(C, r, correction, add_intercept),
                            axis='columns',
                            result_type='expand')

    pdf = df.mapInPandas(map_func, StructType(result_fields)).toPandas()
    phenotypes = phenotype_df.columns.to_series().astype('str')
    n_covar = C.shape[1]

    if offset_type != gwas_fx._OffsetType.LOCO_OFFSET:
        state = _pdf_to_log_reg_state(pdf, phenotypes, n_covar)
        return state

    all_contigs = pdf['contigName'].unique()
    return {
        contig: _pdf_to_log_reg_state(pdf.loc[pdf.contigName == contig, :], phenotypes, n_covar)
        for contig in all_contigs
    }


def _logistic_residualize(X: NDArray[(Any, Any), Float], C: NDArray[(Any, Any), Float],
                          Y_mask: NDArray[(Any, Any), bool], gamma: NDArray[(Any, Any), Float],
                          inv_CtGammaC: NDArray[(Any, Any), Float]) -> NDArray[(Any, Any), Float]:
    '''
    Residualize the genotype vectors given the null model predictions.
    X_res = X - C(C.T gamma C)^-1 C.T gamma X
    '''
    X_hat = gwas_fx._einsum('ic,pcd,ds,sp,sg,sp->igp', C, inv_CtGammaC, C.T, gamma, X, Y_mask)
    return X[:, :, None] - X_hat


def _logistic_regression_inner(genotype_pdf: pd.DataFrame, log_reg_state: LogRegState,
                               C: NDArray[(Any, Any), Float], Y: NDArray[(Any, Any), Float],
                               Y_mask: NDArray[(Any, Any),
                                               bool], Q: Optional[NDArray[(Any, Any),
                                                                          Float]], correction: str,
                               pvalue_threshold: float, phenotype_names: pd.Series) -> pd.DataFrame:
    '''
    Tests a block of genotypes for association with binary traits. We first residualize
    the genotypes based on the null model fit, then perform a fast score test to check for
    possible significance.

    We use semantic indices for the einsum expressions:
    s, i: sample (or individual)
    g: genotype
    p: phenotype
    c, d: covariate
    '''
    X = np.column_stack(genotype_pdf[_VALUES_COLUMN_NAME].array)

    # For approximate Firth correction, we perform a linear residualization
    if correction == correction_approx_firth:
        X = gwas_fx._residualize_in_place(X, Q)

    with oe.shared_intermediates():
        X_res = _logistic_residualize(X, C, Y_mask, log_reg_state.gamma, log_reg_state.inv_CtGammaC)
        num = gwas_fx._einsum('sgp,sp->pg', X_res, log_reg_state.Y_res)**2
        denom = gwas_fx._einsum('sgp,sgp,sp->pg', X_res, X_res, log_reg_state.gamma)
    chisq = np.ravel(num / denom)
    p_values = stats.chi2.sf(chisq, 1)

    del genotype_pdf[_VALUES_COLUMN_NAME]
    out_df = pd.concat([genotype_pdf] * log_reg_state.Y_res.shape[1])
    out_df['chisq'] = list(np.ravel(chisq))
    out_df['pvalue'] = list(np.ravel(p_values))
    out_df['phenotype'] = phenotype_names.repeat(genotype_pdf.shape[0]).tolist()

    if correction != correction_none:
        out_df['correctionSucceeded'] = None
        correction_indices = list(np.where(out_df['pvalue'] < pvalue_threshold)[0])
        if correction == correction_approx_firth:
            out_df['effect'] = np.nan
            out_df['stderror'] = np.nan
            for correction_idx in correction_indices:
                snp_idx = correction_idx % X.shape[1]
                pheno_idx = int(correction_idx / X.shape[1])
                approx_firth_snp_fit = af.correct_approx_firth(
                    X[:, snp_idx], Y[:, pheno_idx], log_reg_state.firth_offset[:, pheno_idx],
                    Y_mask[:, pheno_idx])
                if approx_firth_snp_fit is None:
                    out_df.correctionSucceeded.iloc[correction_idx] = False
                else:
                    out_df.correctionSucceeded.iloc[correction_idx] = True
                    out_df.effect.iloc[correction_idx] = approx_firth_snp_fit.effect
                    out_df.stderror.iloc[correction_idx] = approx_firth_snp_fit.stderror
                    out_df.chisq.iloc[correction_idx] = approx_firth_snp_fit.chisq
                    out_df.pvalue.iloc[correction_idx] = approx_firth_snp_fit.pvalue

    return out_df
