from pyspark.sql.types import StringType, StructField
from python.glow.gwas.tests.test_linear_regression import assert_glow_equals_golden
from typing import Any, Callable, Union
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

__all__ = ['logistic_regression']

fallback_none = 'none'

@typechecked
def logistic_regression(genotype_df: DataFrame,
    phenotype_df: pd.DataFrame,
    covariate_df: pd.DataFrame = pd.DataFrame({}),
    offset_df: pd.DataFrame = pd.DataFrame({}),
    fallback: str = 'none', # TODO: Make approx-firth default
    fit_intercept: bool = True,
    values_column: str = 'values',
    dt: type = np.float64):

    gwas_fx._check_spark_version(genotype_df.sql_ctx.sparkSession)
    gwas_fx._validate_covariates_and_phenotypes(covariate_df, phenotype_df, is_binary=True)
    sql_type = gwas_fx._regression_sql_type(dt)
    genotype_df = gwas_fx._prepare_genotype_df(genotype_df, values_column, sql_type)
    result_fields = [
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
    state = gwas_fx._loco_make_state(Y, phenotype_df, offset_df,
        lambda a, b, c: assemble_log_reg_state(a, b, c, C, Y_mask))
    def map_func(pdf_iterator):
        for pdf in pdf_iterator:
            yield gwas_fx._loco_dispatch(pdf, state, _logistic_regression_inner, C, 
                Y_mask.astype(np.float64), fallback, phenotype_df.columns.to_series().astype('str'))

    return genotype_df.mapInPandas(map_func, result_struct)

def logistic_null_model(y, X, y_mask, offset):
    model = sm.GLM(y[y_mask], X[y_mask], family=sm.families.Binomial(), offset=offset, missing='ignore')
    fit_result = model.fit()
    predictions = model.predict(fit_result.params)
    remapped_predictions = np.zeros(y.shape)
    remapped_predictions[y_mask] = predictions
    return remapped_predictions

def assemble_log_reg_state(Y, phenotype_df, offset_df, C, Y_mask):
    Y_pred = np.row_stack([
        logistic_null_model(Y[:, i], C, Y_mask[:, i], offset_df.iloc[:, i].to_numpy() if offset_df is not None else None) 
        for i in range(Y.shape[1])])
    gamma = Y_pred * (1 - Y_pred)
    CtGammaC = C.T @ (gamma[:, :, None] * C)
    CtGammaC_inv = np.linalg.inv(CtGammaC)
    return LogRegState(CtGammaC_inv, gamma, (Y - Y_pred.T) * Y_mask)


@dataclass
class LogRegState:
    inv_CtGammaC: NDArray[(Any, Any), Float]
    gamma: NDArray[(Any, Any), Float]
    Y_res: NDArray[(Any, Any), Float]

def logistic_residualize(X, C, Y_mask, gamma, inv_CtGammaC):
    '''
    G_res = G - C(C.T gamma C)^-1 C.T gamma G
    '''
    X_hat = oe.contract('ac,pcd,ds,ps,sg,sp->agp', C, inv_CtGammaC, C.T, gamma, X, Y_mask, optimize='dp')
    return X[:, :, None] - X_hat

def _logistic_regression_inner(genotype_pdf, score_test_state, C, Y_mask, fallback_method, phenotype_names):
    X = np.column_stack(genotype_pdf[_VALUES_COLUMN_NAME].array)
    with oe.shared_intermediates():
        X_res = logistic_residualize(X, C, Y_mask, score_test_state.gamma, score_test_state.inv_CtGammaC)
        num = oe.contract('sgp,sp->pg', X_res, score_test_state.Y_res) ** 2
        denom = oe.contract('sgp,sgp,ps->pg', X_res, X_res, score_test_state.gamma)
    t_values = np.ravel(num / denom)
    p_values = stats.chi2.sf(t_values, 1)

    if fallback_method != fallback_none:
        # TODO: Call approx firth here
        ()

    del genotype_pdf[_VALUES_COLUMN_NAME]
    out_df = pd.concat([genotype_pdf] * score_test_state.Y_res.shape[1])
    out_df['tvalue'] = list(np.ravel(t_values))
    out_df['pvalue'] = list(np.ravel(p_values))
    out_df['phenotype'] = phenotype_names.repeat(genotype_pdf.shape[0]).tolist()
    return out_df
