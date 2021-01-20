from typing import Any, Optional
import numpy as np
from dataclasses import dataclass
import scipy
from typeguard import typechecked
from nptyping import Float, NDArray
from scipy import stats


@dataclass
class LogLikelihood:
    pi: NDArray[(Any, ), Float]  # n_samples
    I: NDArray[(Any, Any), Float]  # Fisher information matrix
    deviance: Float  # -2 * penalized log likelihood


@dataclass
class FirthFit:
    beta: NDArray[(Any, ), Float]  # n_covariates for null fit, 1 for SNP fit
    log_likelihood: LogLikelihood


@dataclass
class FirthStatistics:
    effect: Float
    stderror: Float
    chisq: Float
    pvalue: Float


@dataclass
class Model:
    X: NDArray[(Any, Any), Float]
    y: NDArray[(Any, ), Float]
    offset: NDArray[(Any, ), Float]


@typechecked
def _calculate_log_likelihood(beta: NDArray[(Any, ), Float], model: Model) -> LogLikelihood:

    pi = scipy.special.expit(model.X @ beta + model.offset)
    p = pi * (1 - pi)
    I = model.X.T @ (p[:, None] * model.X)
    _, log_abs_det = np.linalg.slogdet(I)
    unpenalized_log_likelihood = np.sum(model.y * np.log(pi) + (1 - model.y) * np.log(1 - pi))

    penalty = 0.5 * log_abs_det
    deviance = -2 * (unpenalized_log_likelihood + penalty)
    return LogLikelihood(pi, I, deviance)


def _fit_firth(beta_init: NDArray[(Any, ), Float],
               X: NDArray[(Any, Any), Float],
               y: NDArray[(Any, ), Float],
               offset: NDArray[(Any, ), Float],
               convergence_limit: float = 1e-5,
               deviance_tolerance: float = 1e-6,
               max_iter: int = 250,
               max_step_size: int = 5,
               max_half_steps: int = 25) -> Optional[FirthFit]:
    '''
    Firth’s bias-reduced penalized-likelihood logistic regression, based on the regenie implementation:
    https://www.biorxiv.org/content/10.1101/2020.06.19.162354v2

    Args:
        beta_init : Initial beta values
        X : Independent variable (covariate for null fit, genotype for SNP fit)
        y : Dependent variable (phenotype)
        offset : Phenotype offset only for null fit, phenotype + covariate effects for SNP fit
        convergence_limit : Convergence is reached if all entries of the penalized score have smaller magnitude
        deviance_tolerance : Non-inferiority margin when halving step size (default from regenie)
        max_iter : Maximum number of Firth iterations (default from regenie)
        max_step_size : Maximum step size during a Firth iteration (default from regenie)
        max_half_steps : Maximum number of half-steps during a Firth iteration (default from regenie)

    Returns:
        None if the fit failed. Otherwise, a FirthFit object containing the fit information.
    '''

    n_iter = 0
    beta = beta_init.copy()
    model = Model(X, y, offset)
    log_likelihood = _calculate_log_likelihood(beta, model)

    while n_iter < max_iter:
        invI = np.linalg.pinv(log_likelihood.I)

        # build hat matrix
        rootG = np.sqrt(log_likelihood.pi * (1 - log_likelihood.pi))
        rootG_X = rootG[:, None] * X  # equivalent to sqrt(diagflat(pi * (1 - pi))) @ X
        h = np.sum((rootG_X @ invI) * rootG_X, axis=1)

        # modified score function
        U = X.T @ (y - log_likelihood.pi + h * (0.5 - log_likelihood.pi))

        # f' / f''
        delta = invI @ U

        # force absolute step size to be less than max_step_size for each entry of beta
        mx = np.amax(np.abs(delta)) / max_step_size
        if mx > 1:
            delta /= mx

        # if the penalized log likelihood decreased, recompute with step-halving
        n_half_steps = 0
        while n_half_steps < max_half_steps:
            new_log_likelihood = _calculate_log_likelihood(beta + delta, model)
            if new_log_likelihood.deviance < log_likelihood.deviance + deviance_tolerance:
                break
            delta /= 2
            n_half_steps += 1

        beta += delta
        log_likelihood = new_log_likelihood

        if np.amax(np.abs(U)) < convergence_limit:
            break

        n_iter += 1

    if n_iter == max_iter:
        # Failed to converge
        return None

    return FirthFit(beta, log_likelihood)


@typechecked
def perform_null_firth_fit(
    y: NDArray[(Any, ), Float],
    C: NDArray[(Any, Any), Float],
    mask: NDArray[(Any, ), bool],
    offset: Optional[Any],  # Typeguard doesn't work with optional NDArrays
    includes_intercept: bool
) -> NDArray[(Any, ), Float]:
    '''
    Performs the null fit for approximate Firth in order to calculate the covariate effects to be
    used as an offset during the SNP fits.

    Args:
        y : Dependent variable (phenotype)
        C : Covariate matrix
        mask : Missingness array; false if the sample is missing a phenotype value, true otherwise
        offset : Phenotype offset
        includes_intercept : True if the first column of the covariate matrix C represents an intercept term

    Returns:
        None if the Firth fit did not converge.
        Otherwise, offset vector with per-sample covariate effects for SNP fits.
    '''

    firth_offset = np.zeros(y.shape)
    if offset is None:
        offset = np.zeros(y.shape)

    masked_y = y[mask]
    masked_C = C[mask, :]
    masked_offset = offset[mask]

    b0_null_fit = np.zeros(C.shape[1])
    if includes_intercept:
        b0_null_fit[0] = (0.5 + masked_y.sum()) / (mask.sum() + 1)
        b0_null_fit[0] = np.log(b0_null_fit[0] / (1 - b0_null_fit[0])) - masked_offset.mean()
    firth_fit_result = _fit_firth(b0_null_fit, masked_C, masked_y, masked_offset)
    if firth_fit_result is None:
        raise ValueError("Null fit failed!")
    firth_offset[mask] = masked_offset + masked_C @ firth_fit_result.beta

    return firth_offset


# Skip typechecking for optimization
def correct_approx_firth(x: NDArray[(Any, ), Float], y: NDArray[(Any, ), Float],
                         firth_offset: NDArray[(Any, ), Float],
                         mask: NDArray[(Any, ), bool]) -> Optional[FirthStatistics]:
    '''
    Calculate LRT statistics for a SNP using the approximate Firth method.

    Args:
        x : Genotypes for SNP
        y : Dependent variable (phenotype)
        offset : Phenotype offset including covariate effects from the null model fit
        mask : Missingness array; false if the sample is missing a phenotype value, true otherwise

    Returns:
        None if the Firth fit did not converge.
        Otherwise, likelihood-ratio test statistics.
    '''

    beta_init = np.zeros(1)
    masked_y = y[mask]
    masked_X = x[mask, None]
    masked_offset = firth_offset[mask]
    firth_fit = _fit_firth(beta_init, masked_X, masked_y, masked_offset)
    if firth_fit is None:
        return None

    effect = firth_fit.beta.item()
    # Likelihood-ratio test
    null_model = Model(masked_X, masked_y, masked_offset)
    null_deviance = _calculate_log_likelihood(beta_init, null_model).deviance
    chisq = null_deviance - firth_fit.log_likelihood.deviance
    pvalue = stats.chi2.sf(chisq, 1)
    # Based on the Hessian of the unpenalized log-likelihood
    stderror = np.sqrt(np.linalg.pinv(firth_fit.log_likelihood.I).diagonal()[-1])
    return FirthStatistics(effect=effect, stderror=stderror, chisq=chisq, pvalue=pvalue)
