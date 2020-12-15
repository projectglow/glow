from typing import Any, Optional
import pandas as pd
from pandas import Series
import numpy as np
from dataclasses import dataclass
from typeguard import typechecked
from nptyping import Float, NDArray
from scipy import stats


def _calculate_log_likelihood(beta, X, y, offset):
    pi = 1 - 1 / (np.exp(X @ beta + offset) + 1)
    G = np.diagflat(pi * (1-pi))
    I = np.atleast_2d(X.T @ G @ X) # fisher information matrix
    LL_matrix = np.atleast_2d(y @ np.log(pi) + (1-y) @ np.log(1-pi))
    _, logdet = np.linalg.slogdet(I)
    penalized_LL = np.sum(LL_matrix) + 0.5 * logdet
    return (pi, G, I, LL_matrix, penalized_LL)


def _fit_firth_logistic(beta_init, X, y, offset, tolerance=1e-5, max_iter=250, max_step_size=5, max_half_steps=25):
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
                raise ValueError("Too many half-steps!")
            delta /= 2
            new_beta = beta + delta
            pi, G, I, new_LL_matrix, new_penalized_LL = _calculate_log_likelihood(new_beta, X, y, offset)
            n_half_steps += 1

        beta = new_beta
        LL_matrix = new_LL_matrix
        penalized_LL = new_penalized_LL

        n_iter += 1

    if n_iter == max_iter:
        raise ValueError("Too many iterations!")

    return beta, LL_matrix, penalized_LL


@dataclass
class ApproxFirthState:
    penalized_LL_null_fit: NDArray[(Any), Float]
    logit_offset: NDArray[(Any), Float]


@typechecked
def assemble_approx_firth_state(
        Y: NDArray[(Any, Any), Float],
        offset_df: Optional[pd.DataFrame],
        C: NDArray[(Any, Any), Float],
        Y_mask: NDArray[(Any, Any), Float]) -> ApproxFirthState:

    num_Y = Y.shape[1]
    penalized_LL_null_fit = np.zeros(num_Y)
    logit_offset = np.zeros(num_Y)

    for i in range(num_Y):
        y = Y[:, i]
        y_mask = Y_mask[:, i]
        offset = offset_df.iloc[:, i].to_numpy() if offset_df is not None else np.zeros(y.shape)
        b0_null_fit = np.zeros(1 + C.shape(0))
        b0_null_fit[0] = (0.5 + y.sum()) / (y_mask.sum() + 1)
        b0_null_fit[0] = np.log(b0_null_fit[0] / (1 - b0_null_fit[0])) - offset.mean()
        b_null_fit, _, penalized_LL_null_fit[i] = _fit_firth_logistic(b0_null_fit, C, y, offset)
        logit_offset[i] = offset + (C.values * b_null_fit).sum(axis=1)

    return ApproxFirthState(penalized_LL_null_fit, logit_offset)


def correct_approx_firth(x_res, y_res, logit_offset, penalized_LL_null_fit) -> Series:
    b_snp_fit, snp_LL_matrix, snp_penalized_LL = _fit_firth_logistic(
        np.zeros(1),
        x_res,
        y_res,
        logit_offset
    )
    tvalue = 2 * (snp_penalized_LL - penalized_LL_null_fit)
    pvalue = stats.chi2.sf(tvalue, 1)
    effect = b_snp_fit.item()
    stderr = np.linalg.pinv(snp_LL_matrix).item()
    return Series({'tvalue': tvalue, 'pvalue': pvalue, 'effect': effect, 'stderr': stderr})
