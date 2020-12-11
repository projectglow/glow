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

@typechecked
def logistic_regression(genotype_df: DataFrame,
    phenotype_df: pd.DataFrame,
    covariate_df: pd.DataFrame = pd.DataFrame({}),
    offset_df: pd.DataFrame = pd.DataFrame({}),
    fallback_method: Union[str, Callable] = 'approx-firth',
    fit_intercept: bool = True,
    values_column: str = 'values',
    dt: type = np.float64):

    gwas_fx._check_spark_version(genotype_df.sql_ctx.sparkSession)
    gwas_fx._validate_covariates_and_phenotypes(covariate_df, phenotype_df, is_binary=True)
    sql_type = gwas_fx._regression_sql_type(dt)
    genotype_df = gwas_fx._prepare_genotype_df(genotype_df, values_column, sql_type)
    C = covariate_df.to_numpy(dt, copy=True)


def logistic_null_model(Y, X, Y_mask, offset):
    model = sm.GLM(Y, X, family=sm.families.Binomial(), offset=offset, missing='drop')
    fit_result = model.fit()
    return model.predict(fit_result.params)

def assemble_covariate_state(C, Y, Y_mask):
    Y_pred = np.row_stack([logistic_null_model(col, C, None) for col in Y.T])
    gamma = Y_pred * (1 - Y_pred)
    CtGammaC = C.T @ (gamma[:, :, None] * C)
    CtGammaC_inv = np.linalg.inv(CtGammaC)
    return ScoreTestState(CtGammaC_inv, gamma, Y.T - Y_pred.T)


@dataclass
class ScoreTestState:
    inv_CtGammaC: NDArray[(Any, Any), Float]
    gamma: NDArray[(Any, Any), Float]
    Y_res: NDArray[(Any, Any), Float]

def logistic_residualize(X, C, gamma, inv_CtGammaC):
    '''
    G_res = G - C(C.T gamma C)^-1 C.T gamma G
    '''
    X_hat = oe.contract('ac,pcd,ds,ps,sg->agp', C, inv_CtGammaC, C.T, gamma, X, optimize='dp')
    return X[:, :, None] - X_hat

def score_test(genotype_pdf, C, score_test_state, phenotype_names, dt=np.float64):
    X = np.column_stack(genotype_pdf[_VALUES_COLUMN_NAME].array)
    with oe.shared_intermediates():
        X_res = logistic_residualize(X, C, score_test_state.gamma, score_test_state.inv_CtGammaC)
        num = oe.contract('sgp,sp->pg', X_res, score_test_state.Y_res) ** 2
        denom = oe.contract('sgp,sgp,ps->pg', X_res, X_res, score_test_state.gamma)
    t_values = np.ravel(num / denom)
    p_values = stats.chi2.sf(t_values, 1)

    del genotype_pdf[_VALUES_COLUMN_NAME]
    out_df = pd.concat([genotype_pdf] * score_test_state.Y_res.shape[1])
    out_df['tvalue'] = list(np.ravel(t_values))
    out_df['pvalue'] = list(np.ravel(p_values))
    out_df['phenotype'] = phenotype_names.repeat(genotype_pdf.shape[0]).tolist()
    return out_df
