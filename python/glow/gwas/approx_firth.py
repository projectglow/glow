from typing import Any, Optional
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typeguard import typechecked
from nptyping import Float, NDArray
from scipy import stats


@dataclass
class LogLikelihood:
    pi: NDArray[(Any,), Float]
    G: NDArray[(Any, Any), Float] # diag(pi(1-pi))
    I: NDArray[(Any, Any), Float] # Fisher information matrix, X'GX
    deviance: Float # -2 * penalized log likelihood


@dataclass
class FirthFit:
    beta: NDArray[(Any,), Float]
    log_likelihood: LogLikelihood


@dataclass
class ApproxFirthState:
    logit_offset: NDArray[(Any, Any), Float]


@dataclass
class FirthStatistics:
    effect: Float
    stderr: Float
    tvalue: Float
    pvalue: Float


@typechecked
def _calculate_log_likelihood(
        beta: NDArray[(Any,), Float],
        X: NDArray[(Any, Any), Float],
        y: NDArray[(Any,), Float],
        offset: NDArray[(Any,), Float],
        y_mask: NDArray[(Any,), Float],
        eps: float = 0) -> LogLikelihood:

    pi = 1 - 1 / (np.exp(X @ beta + offset * y_mask) + 1)
    G = np.diagflat(pi * (1-pi) * y_mask)
    I = np.atleast_2d(X.T @ G @ X)

    unpenalized_log_likelihood = y_mask * (y @ np.log(pi + eps) + (1-y) @ np.log(1-pi + eps))
    _, log_abs_det = np.linalg.slogdet(I)
    penalty = 0.5 * log_abs_det
    deviance = -2 * (unpenalized_log_likelihood + penalty)
    return LogLikelihood(pi, G, I, deviance)


@typechecked
def _fit_firth(
        beta_init: NDArray[(Any,), Float],
        X: NDArray[(Any, Any), Float],
        y: NDArray[(Any,), Float],
        offset: NDArray[(Any,), Float],
        y_mask: NDArray[(Any,), Float],
        convergence_limit: float = 1e-5,
        deviance_tolerance: float = 1e-6,
        max_iter: int = 250,
        max_step_size: int = 5,
        max_half_steps: int = 25) -> Optional[FirthFit]:
    '''
    Firth’s bias-reduced penalized-likelihood logistic regression, based on the regenie implementation.

    :param beta_init: Initial beta values
    :param X: Independent variable (covariate for null fit, genotype for SNP fit)
    :param y: Dependent variable (phenotype)
    :param offset: Offset (phenotype offset only for null fit, also with covariate offset for SNP fit)
    :param convergence_limit: Convergence is reached if all entries of the penalized score have smaller magnitude
    :param deviance_tolerance: Non-inferiority margin when halving step size
    :param max_iter: Maximum number of Firth iterations
    :param max_step_size: Maximum step size during a Firth iteration
    :param max_half_steps: Maximum number of half-steps during a Firth iteration
    :return: None if the fit failed
    '''

    n_iter = 0
    beta = beta_init
    log_likelihood = _calculate_log_likelihood(beta, X, y, offset, y_mask)
    while n_iter < max_iter:
        invI = np.linalg.pinv(log_likelihood.I)

        # build hat matrix
        rootG_X = np.sqrt(log_likelihood.G) @ X
        h = np.diagonal(rootG_X @ invI @ rootG_X.T)
        U = X.T @ (y_mask * (y - log_likelihood.pi + h * (0.5 - log_likelihood.pi)))

        # print(f"Considering stopping... {U}")
        if np.amax(np.abs(U)) < convergence_limit:
            # print(f"Stopping! {U}")
            break

        # f' / f''
        delta = invI @ U

        # force absolute step size to be less than max_step_size for each entry of beta
        step_size = np.amax(np.abs(delta))
        # print(f"Delta is {delta}")
        mx = step_size / max_step_size
        if mx > 1:
            delta = delta / mx

        new_log_likelihood = _calculate_log_likelihood(beta + delta, X, y, offset, y_mask)

        # if the penalized log likelihood decreased, recompute with step-halving
        n_half_steps = 0
        # print(f"New deviance is {new_log_likelihood.deviance}, was {log_likelihood.deviance} before")
        while new_log_likelihood.deviance >= log_likelihood.deviance + deviance_tolerance:
            if n_half_steps == max_half_steps:
                print(f"Exceeded half-step limit {max_half_steps}")
                return None
            delta /= 2
            new_log_likelihood = _calculate_log_likelihood(beta + delta, X, y, offset, y_mask)
            # print(f"Halved {n_half_steps} times: new deviance {new_log_likelihood.deviance}, old deviance {log_likelihood.deviance}")
            n_half_steps += 1

        # print(f"Beta iter {n_iter}: {beta}")
        beta = beta + delta
        log_likelihood = new_log_likelihood

        n_iter += 1

    if n_iter == max_iter:
        print(f"Exceeded iteration limit {max_iter}")
        return None

    return FirthFit(beta, log_likelihood)


@typechecked
def create_approx_firth_state(
        Y: NDArray[(Any, Any), Float],
        offset_df: Optional[pd.DataFrame],
        C: NDArray[(Any, Any), Float],
        Y_mask: NDArray[(Any, Any), Float],
        fit_intercept: bool) -> ApproxFirthState:
    '''
    Performs the null fit for approximate Firth.

    :return: Offset with covariate effects for SNP fit
    '''

    num_Y = Y.shape[1]
    logit_offset = np.zeros(Y.shape)

    for i in range(num_Y):
        y = Y[:, i]
        y_mask = Y_mask[:, i]
        offset = offset_df.iloc[:, i].to_numpy() if offset_df is not None else np.zeros(y.shape)
        b0_null_fit = np.zeros(C.shape[1])
        if fit_intercept:
            b0_null_fit[-1] = (0.5 + y.sum()) / (y_mask.sum() + 1)
            b0_null_fit[-1] = np.log(b0_null_fit[-1] / (1 - b0_null_fit[-1])) - offset.mean()
        # In regenie, this may retry with max_step_size=5, max_iter=5000
        firth_fit_result = _fit_firth(b0_null_fit, C, y, offset, y_mask)
        if firth_fit_result is None:
            raise ValueError("Null fit failed!")
        logit_offset[:, i] = offset + (C @ firth_fit_result.beta)

    return ApproxFirthState(logit_offset)


@typechecked
def correct_approx_firth(
        x_res: NDArray[(Any,), Float],
        y_res: NDArray[(Any,), Float],
        logit_offset: NDArray[(Any,), Float],
        y_mask: NDArray[(Any,), Float]) -> Optional[FirthStatistics]:
    '''
    Calculate LRT statistics for a SNP using the approximate Firth method.

    :return: None if the Firth fit did not converge, LRT statistics otherwise
    '''

    beta_init = np.zeros(1)
    X = np.expand_dims(x_res, axis=1)
    firth_fit = _fit_firth(beta_init, X, y_res, logit_offset, y_mask)
    if firth_fit is None:
        return None

    effect = firth_fit.beta.item()
    # Likelihood-ratio test
    null_deviance = _calculate_log_likelihood(beta_init, X, y_res, logit_offset, y_mask).deviance
    tvalue = -1 * (firth_fit.log_likelihood.deviance - null_deviance)
    pvalue = stats.chi2.sf(tvalue, 1)
    # Based on the Hessian of the unpenalized log-likelihood
    stderr = np.sqrt(np.linalg.pinv(firth_fit.log_likelihood.I).diagonal()[-1])
    return FirthStatistics(effect=effect, stderr=stderr, tvalue=tvalue, pvalue=pvalue)
