from typing import Any, Optional
import pandas as pd
from pandas import Series
import numpy as np
from dataclasses import dataclass
from typeguard import typechecked
from nptyping import Float, NDArray
from scipy import stats


def _calculate_log_likelihood(
        beta: NDArray[(Any,), Float],
        X: NDArray[(Any, Any), Float],
        y: NDArray[(Any,), Float],
        offset: NDArray[(Any,), Float]):

    pi = 1 - 1 / (np.exp(X @ beta + offset) + 1)
    G = np.diagflat(pi * (1-pi))
    I = np.atleast_2d(X.T @ G @ X) # fisher information matrix
    eps = 1e-15 # Avoid log underflow
    LL_matrix = np.atleast_2d(y @ np.log(pi + eps) + (1-y) @ np.log(1-pi + eps))
    _, logdet = np.linalg.slogdet(I)
    penalized_LL = np.sum(LL_matrix) + 0.5 * logdet
    return (pi, G, I, LL_matrix, penalized_LL)


@dataclass
class FirthFitResults:
    beta: NDArray[(Any,), Float]
    LL_matrix: NDArray[(Any, Any), Float]
    penalized_LL: Float


def _fit_firth_logistic(
        beta_init: NDArray[(Any,), Float],
        X: NDArray[(Any, Any), Float],
        y: NDArray[(Any,), Float],
        offset: NDArray[(Any,), Float],
        tolerance=1e-5,
        max_iter=250,
        max_step_size=25,
        max_half_steps=25) -> Optional[FirthFitResults]:

    n_iter = 0
    beta = beta_init
    pi, G, I, LL_matrix, penalized_LL = _calculate_log_likelihood(beta, X, y, offset)
    while n_iter < max_iter:
        # inverse of the fisher information matrix
        invI = np.linalg.pinv(I)

        # build hat matrix
        rootG_X = np.sqrt(G) @ X
        H = rootG_X @ invI @ rootG_X.T
        h = np.diagonal(H)

        # penalised score
        U = X.T @ (y - pi + h * (0.5 - pi))
        if np.amax(np.abs(U)) < tolerance:
            break

        # f' / f''
        delta = invI @ U

        # force absolute step size to be less than max_step_size for each entry of beta
        mx = np.amax(np.abs(delta)) / max_step_size
        if mx > 1:
            delta = delta / mx

        new_beta = beta + delta
        pi, G, I, new_LL_matrix, new_penalized_LL = _calculate_log_likelihood(new_beta, X, y, offset)

        # if the penalized log likelihood decreased, recompute with step-halving
        n_half_steps = 0
        while new_penalized_LL < penalized_LL:
            if n_half_steps == max_half_steps:
                print("Too many half-steps!")
                return None
            delta /= 2
            new_beta = beta + delta
            pi, G, I, new_LL_matrix, new_penalized_LL = _calculate_log_likelihood(new_beta, X, y, offset)
            n_half_steps += 1

        beta = new_beta
        LL_matrix = new_LL_matrix
        penalized_LL = new_penalized_LL

        n_iter += 1

    if n_iter == max_iter:
        print("Too many iterations!")
        return None

    return FirthFitResults(beta, LL_matrix, penalized_LL)


@dataclass
class ApproxFirthState:
    logit_offset: NDArray[(Any, Any), Float]
    penalized_LL_null_fit: NDArray[(Any,), Float]


@typechecked
def create_approx_firth_state(
        Y: NDArray[(Any, Any), Float],
        offset_df: Optional[pd.DataFrame],
        C: NDArray[(Any, Any), Float],
        Y_mask: NDArray[(Any, Any), Float],
        fit_intercept: bool) -> ApproxFirthState:

    num_Y = Y.shape[1]
    penalized_LL_null_fit = np.zeros(num_Y)
    logit_offset = np.zeros(Y.shape)

    for i in range(num_Y):
        y = Y[:, i]
        y_mask = Y_mask[:, i]
        offset = offset_df.iloc[:, i].to_numpy() if offset_df is not None else np.zeros(y.shape)
        b0_null_fit = np.zeros(C.shape[1])
        if fit_intercept:
            b0_null_fit[-1] = (0.5 + y.sum()) / (y_mask.sum() + 1)
            b0_null_fit[-1] = np.log(b0_null_fit[-1] / (1 - b0_null_fit[-1])) - offset.mean()
        firthFitResult = _fit_firth_logistic(
            b0_null_fit, C, y, offset, max_step_size=5, max_iter=5000)
        if firthFitResult is None:
            raise ValueError("Null fit failed!")
        penalized_LL_null_fit[i] = firthFitResult.penalized_LL
        logit_offset[:, i] = offset + (C @ firthFitResult.beta)

    return ApproxFirthState(logit_offset, penalized_LL_null_fit)


def correct_approx_firth(
        x_res: NDArray[(Any,), Float],
        y_res: NDArray[(Any,), Float],
        logit_offset: NDArray[(Any,), Float],
        penalized_LL_null_fit: Float) -> Optional[Series]:

    firthFitResult = _fit_firth_logistic(
        np.zeros(1),
        np.expand_dims(x_res, axis=1),
        y_res,
        logit_offset
    )
    if firthFitResult is None:
        return None
    tvalue = 2 * (firthFitResult.penalized_LL - penalized_LL_null_fit)
    pvalue = stats.chi2.sf(tvalue, 1)
    effect = firthFitResult.beta.item()
    stderr = np.linalg.pinv(firthFitResult.LL_matrix).item()
    return Series({'tvalue': tvalue, 'pvalue': pvalue, 'effect': effect, 'stderr': stderr})
