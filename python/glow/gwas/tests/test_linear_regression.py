import statsmodels.api as sm

def run_linear_regression(genotype_df, phenotype_df, covariate_df, fit_intercept=True):
    C = covariate_df.to_numpy('float64', copy=True)
    if fit_intercept:
        C = np.hstack((np.ones(C.shape[0]), C))
    Q = np.linalg.qr(C)[0]
    Y = phenotype_df.to_numpy('float64', copy=True)
    Y_mask = ~np.isnan(Y)
    Y[~Y_mask] = 0
    QtY = Q.T @ Y
    YdotY = np.sum(Y * Y, axis = 0) - np.sum(QtY * QtY, axis = 0)
    dof = c.shape[0] - c.shape[1] - 1

    return _linear_regression_inner(pdf, Y, Q, QtY, YdotY, Y_mask, dof)

def statsmodels_baseline(genotype_df, phenotype_df, covariate_df, fit_intercept=True):
    