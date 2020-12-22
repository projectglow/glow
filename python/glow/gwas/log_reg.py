from pyspark.sql.types import StringType, StructField
from typing import Any, Optional
import pandas as pd
import numpy as np
from pyspark.sql import DataFrame
import statsmodels.api as sm
from dataclasses import dataclass
from typeguard import typechecked
from nptyping import Float, NDArray
from scipy import stats
import opt_einsum as oe
from . import functions as gwas_fx
from .functions import _VALUES_COLUMN_NAME
from .approx_firth_correction import *

__all__ = ['logistic_regression']

correction_none = 'none'
correction_approx_firth = 'approx-firth'


@typechecked
def logistic_regression(
        genotype_df: DataFrame,
        phenotype_df: pd.DataFrame,
        covariate_df: pd.DataFrame = pd.DataFrame({}),
        offset_df: pd.DataFrame = pd.DataFrame({}),
        correction: str = correction_approx_firth,
        pvalue_threshold: float = 0.05,
        fit_intercept: bool = True,
        values_column: str = 'values',
        dt: type = np.float64) -> DataFrame:
    '''
    Uses logistic regression to test for association between genotypes and one or more binary
    phenotypes. This is a distributed version of the method from regenie:
    https://www.biorxiv.org/content/10.1101/2020.06.19.162354v2

    On the driver node, we fit a logistic regression model based on the covariates for each
    phenotype:

    logit(y) ~ C

    where y is a phenotype vector and C is the covariate matrix.

    We compute the probability predictions h_hat and broadcast the residuals (y - y_hat), gamma vectors
    (where gamma is defined as y_hat * (1 - Y_hat)), and (C.T gamma C)^-1 matrices. In each task,
    we then adjust the new genotypes based on the null fit, perform a score test as a fast scan
    for potentially significant variants, and then test variants with p values below a threshold
    using a more selective, more expensive test.

    Args:
        genotype_df : Spark DataFrame containing genomic data
        phenotype_df : Pandas DataFrame containing phenotypic data
        covariate_df : An optional Pandas DataFrame containing covariates
        offset_df : An optional Pandas DataFrame containing the phenotype offset. This value will be used
                    as a offset in the covariate only and per variant logistic regression models. The ``offset_df`` may
                    have one or two levels of indexing. If one level, the index should be the same as the ``phenotype_df``.
                    If two levels, the level 0 index should be the same as the ``phenotype_df``, and the level 1 index
                    should be the contig name. The two level index scheme allows for per-contig offsets like
                    LOCO predictions from GloWGR.
        correction : Which test to use for variants that meet a significance threshold for the score test
        pvalue_threshold : Variants with a pvalue below this threshold will be tested using the ``correction`` method.
        fit_intercept : Whether or not to add an intercept column to the covariate DataFrame
        values_column : A column name or column expression to test with linear regression. If a column name is provided,
                        ``genotype_df`` should have a column with this name and a numeric array type. If a column expression
                        is provided, the expression should return a numeric array type.
        dt : The numpy datatype to use in the linear regression test. Must be `np.float32` or `np.float64`.

    Returns:
        A Spark DataFrame that contains

        - All columns from ``genotype_df`` except the ``values_column`` and the ``genotypes`` column if one exists
        - ``tvalue``: The chi squared test statistic according to the score test or the correction method
        - ``pvalue``: P value estimated from the test statistic
        - ``phenotype``: The phenotype name as determiend by the column names of ``phenotype_df``
    '''

    gwas_fx._check_spark_version(genotype_df.sql_ctx.sparkSession)
    gwas_fx._validate_covariates_and_phenotypes(covariate_df, phenotype_df, is_binary=True)
    sql_type = gwas_fx._regression_sql_type(dt)
    genotype_df = gwas_fx._prepare_genotype_df(genotype_df, values_column, sql_type)
    result_fields = [
        # TODO: Probably want to put effect size and stderr here for approx-firth
        StructField('tvalue', sql_type),
        StructField('pvalue', sql_type),
        StructField('phenotype', StringType())
    ]

    result_struct = gwas_fx._output_schema(genotype_df.schema.fields, result_fields)
    C = covariate_df.to_numpy(dt, copy=True)
    if fit_intercept:
        C = gwas_fx._add_intercept(C, phenotype_df.shape[0])
    Y = phenotype_df.to_numpy(dt, copy=True)
    Y_mask = ~(np.isnan(Y))
    np.nan_to_num(Y, copy=False)

    log_reg_state = gwas_fx._loco_make_state(
        Y, phenotype_df, offset_df,
        lambda y, pdf, odf: _create_log_reg_state(y, pdf, odf, C, Y_mask, correction, fit_intercept))

    def map_func(pdf_iterator):
        for pdf in pdf_iterator:
            yield gwas_fx._loco_dispatch(pdf, log_reg_state, _logistic_regression_inner, C,
                                         Y_mask.astype(np.float64), correction, pvalue_threshold,
                                         phenotype_df.columns.to_series().astype('str'))

    return genotype_df.mapInPandas(map_func, result_struct)


@typechecked
def _logistic_null_model_predictions(
    y: NDArray[Any, Float],
    X: NDArray[Any, Float],
    y_mask: NDArray[Any, bool],
    # nptying can't handle optional NDArrays, so don't verify the type for offset
    offset
) -> NDArray[Any, Float]:
    if offset is not None:
        offset = offset[y_mask]
    model = sm.GLM(y[y_mask],
                   X[y_mask],
                   family=sm.families.Binomial(),
                   offset=offset,
                   missing='ignore')
    fit_result = model.fit()
    predictions = model.predict(fit_result.params)

    # Store 0 as prediction for samples with missing phenotypes
    remapped_predictions = np.zeros(y.shape)
    remapped_predictions[y_mask] = predictions
    return remapped_predictions


@dataclass
class LogRegState:
    inv_CtGammaC: NDArray[(Any, Any), Float]
    gamma: NDArray[(Any, Any), Float]
    Y_res: NDArray[(Any, Any), Float]
    approx_firth_state: Optional[ApproxFirthState]


@typechecked
def _create_log_reg_state(
        Y: NDArray[(Any, Any), Float],
        phenotype_df: pd.DataFrame,  # Unused, only to share code with lin_reg.py
        offset_df: Optional[pd.DataFrame],
        C: NDArray[(Any, Any), Float],
        Y_mask: NDArray[(Any, Any), Float],
        correction: str,
        fit_intercept: bool) -> LogRegState:
    Y_pred = np.row_stack([
        _logistic_null_model_predictions(
            Y[:, i], C, Y_mask[:, i],
            offset_df.iloc[:, i].to_numpy() if offset_df is not None else None)
        for i in range(Y.shape[1])
    ])
    gamma = Y_pred * (1 - Y_pred)
    CtGammaC = C.T @ (gamma[:, :, None] * C)
    inv_CtGammaC = np.linalg.inv(CtGammaC)

    if correction == correction_approx_firth:
        approx_firth_state = create_approx_firth_state(Y, offset_df, C, Y_mask, fit_intercept)
    else:
        approx_firth_state = None

    return LogRegState(inv_CtGammaC, gamma, (Y - Y_pred.T) * Y_mask, approx_firth_state)


def _logistic_residualize(X: NDArray[(Any, Any), Float], C: NDArray[(Any, Any), Float],
                          Y_mask: NDArray[(Any, Any), Float], gamma: NDArray[(Any, Any), Float],
                          inv_CtGammaC: NDArray[(Any, Any), Float]) -> NDArray[(Any, Any), Float]:
    '''
    Residualize the genotype vectors given the null model predictions.
    X_res = X - C(C.T gamma C)^-1 C.T gamma X
    '''
    X_hat = gwas_fx._einsum('ic,pcd,ds,ps,sg,sp->igp', C, inv_CtGammaC, C.T, gamma, X, Y_mask)
    return X[:, :, None] - X_hat


def _logistic_regression_inner(genotype_pdf: pd.DataFrame, log_reg_state: LogRegState,
                               C: NDArray[(Any, Any), Float], Y_mask: NDArray[(Any, Any), Float],
                               correction: str, pvalue_threshold: float, phenotype_names: pd.Series) -> pd.DataFrame:
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
    with oe.shared_intermediates():
        X_res = _logistic_residualize(X, C, Y_mask, log_reg_state.gamma, log_reg_state.inv_CtGammaC)
        num = gwas_fx._einsum('sgp,sp->pg', X_res, log_reg_state.Y_res)**2
        denom = gwas_fx._einsum('sgp,sgp,ps->pg', X_res, X_res, log_reg_state.gamma)
    t_values = np.ravel(num / denom)
    p_values = stats.chi2.sf(t_values, 1)

    del genotype_pdf[_VALUES_COLUMN_NAME]
    out_df = pd.concat([genotype_pdf] * log_reg_state.Y_res.shape[1])
    out_df['tvalue'] = list(np.ravel(t_values))
    out_df['pvalue'] = list(np.ravel(p_values))
    out_df['phenotype'] = phenotype_names.repeat(genotype_pdf.shape[0]).tolist()

    if correction != correction_none:
        correction_indices = out_df.index[out_df['pvalue'] < pvalue_threshold]
        if correction == correction_approx_firth:
            for correction_idx in correction_indices:
                snp_index = correction_idx % genotype_pdf.shape[0]
                phenotype_index = int(correction_idx / phenotype_names.size)
                out_df.iloc[correction_idx] = correct_approx_firth(
                    X_res[snp_index][phenotype_index],
                    log_reg_state.Y_res[phenotype_index],
                    log_reg_state.approx_firth_state.logit_offset[phenotype_index],
                    log_reg_state.approx_firth_state.penalized_LL_null_fit[phenotype_index],
                )
        else:
            raise ValueError(f"Only supported correction method is {correction_approx_firth}")

    return out_df
