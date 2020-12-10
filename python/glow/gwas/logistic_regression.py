from typing import Any, Callable, Union
import pandas as pd
import numpy as np
from pyspark.sql import DataFrame
import statsmodels.api as sm
from dataclasses import dataclass
from typeguard import typechecked
from nptyping import Float, NDArray
from scipy import stats
from opt_einsum import oe
from . import functions as gwas_fx, gwas

__all__ = ['logistic_regression']

_VALUES_COLUMN_NAME = '_logreg_values'

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
    


def logistic_null_model(Y, X, offset):
    model = sm.GLM(Y, X, family=sm.families.Binomial(), offset=offset, missing='drop')
    fit_result = model.fit()
    return model.predict(fit_result.params)

def assemble_covariate_state(covariate_df, phenotype_df):
    C = covariate_df.to_numpy()
    Y_pred = np.row_stack([logistic_null_model(phenotype_df[p], C, None) for p in phenotype_df])
    gamma = Y_pred * (1 - Y_pred)
    CtGammaC = C.T @ (gamma[:, :, None] * C)
    CtGammaC_inv = np.linalg.inv(CtGammaC)
    return LogisticRegressionState(CtGammaC_inv, gamma, phenotype_df.to_numpy() - Y_pred.T)


@dataclass
class LogisticRegressionState:
    inv_CtGammaC: NDArray[(Any, Any), Float]
    gamma: NDArray[(Any, Any), Float]
    Y_res: NDArray[(Any, Any), Float]


def logistic_residualize(X, C, gamma, inv_CtGammaC):
    '''
    G_res = G - C(C.T gamma C)^-1 C.T gamma G
    '''
    X_hat = np.einsum('sc,pcc,cs,ps,sg->sgp', C, inv_CtGammaC, C.T, gamma, X)
    return X[:, :, None] - X_hat

def logistic_residualize(X, C, gamma, inv_CtGammaC):
    '''
    G_res = G - C(C.T gamma C)^-1 C.T gamma G
    '''
    X_hat = oe.contract('ac,pcd,ds,ps,sg->agp', C, inv_CtGammaC, C.T, gamma, X, optimize='dp')
    return X[:, :, None] - X_hat

def score_test(genotype_df, C, score_test_state, dt=np.float64):
    X = np.column_stack(genotype_df[_VALUES_COLUMN_NAME].to_numpy(dt))
    X_res = logistic_residualize(X, C, score_test_state.gamma, score_test_state.inv_CtGammaC)
    num = np.einsum('sgp, sp->gp', X_res, score_test_state.Y_res) ** 2
    denom = np.einsum('sgp,sgp,sp->gp', X_res, X_res, score_test_state.gamma)
    t_values = np.ravel(num / denom)
    p_values = stats.chi2.sf(t_values, 1)
    return p_values
