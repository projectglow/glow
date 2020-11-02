import numpy as np
import statsmodels.api as sm
import pandas as pd
import glow.gwas.linear_regression as lr

def run_linear_regression(genotype_df, phenotype_df, covariate_df, fit_intercept=True):
    phenotype_names = phenotype_df.columns.astype('str')
    C = covariate_df.to_numpy('float64', copy=True)
    if fit_intercept:
        C = lr._add_intercept(C, genotype_df.shape[0])
    if not C.size:
        C = np.zeros((genotype_df.shape[0], 1))
    Q = np.linalg.qr(C)[0]
    Y = phenotype_df.to_numpy('float64', copy=True)
    Y_mask = ~np.isnan(Y)
    Y[~Y_mask] = 0
    QtY = Q.T @ Y
    YdotY = np.sum(Y * Y, axis = 0) - np.sum(QtY * QtY, axis = 0)
    dof = C.shape[0] - C.shape[1] - 1
    pdf = pd.DataFrame({'values': list(genotype_df.to_numpy().T)})

    return lr._linear_regression_inner(pdf, Y, Q, QtY, YdotY, Y_mask, dof, phenotype_names)

def residualize(Y, X):
    pinv = np.linalg.pinv(X)
    filled = np.nan_to_num(Y)
    coef = pinv @ filled
    print(coef)
    pred = X @ coef
    print(pred)
    return Y - pred

def statsmodels_baseline(genotype_df, phenotype_df, covariate_df, fit_intercept=True):
    # Project out covariates from genotypes and phenotypes
    C = covariate_df.to_numpy('float64')
    num_samples = C.shape[0] if C.shape else genotype_df.shape[0]
    if fit_intercept:
        C = lr._add_intercept(C, num_samples)
    X = genotype_df.to_numpy('float64')
    X = residualize(X, C)
    Y = phenotype_df.to_numpy('float64')
    print(Y)
    Y = residualize(Y, C)
    print(Y)
    phenotype_df.columns = phenotype_df.columns.astype('str')
    dof = C.shape[0] - C.shape[1] - 1
    effects = []
    errors = []
    tvalues = []
    pvalues = []
    for phenotype in Y.T:
        for genotype in X.T:
            genotype = pd.Series(genotype, name='genotype')
            model = sm.OLS(phenotype, genotype, missing='drop')
            model.df_resid = dof
            results = model.fit()
            effects.append(results.params.genotype)
            errors.append(results.bse.genotype)
            tvalues.append(results.tvalues.genotype)
            pvalues.append(results.pvalues.genotype)
    return pd.DataFrame({
        'effect': effects,
        'standard_error': errors,
        'tvalue': tvalues,
        'pvalue': pvalues,
        'phenotype': phenotype_df.columns.to_series().repeat(genotype_df.shape[1])})

def regression_results_equal(df1, df2):
    df1 = df1.sort_values('phenotype', kind='mergesort')
    df2 = df2.sort_values('phenotype', kind='mergesort')
    strings_equal = np.array_equal(df1.phenotype.array, df2.phenotype.array)
    numerics_equal = np.allclose(df1.select_dtypes(exclude=['object']), df2.select_dtypes(exclude=['object']))
    return strings_equal and numerics_equal

def assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df, fit_intercept=True):
    glow = run_linear_regression(genotype_df, phenotype_df, covariate_df, fit_intercept)
    golden = statsmodels_baseline(genotype_df, phenotype_df, covariate_df, fit_intercept)
    assert regression_results_equal(glow, golden)

def test_r_glm_baseline():
    dataset = sm.datasets.get_rdataset('cars').data
    genotype_df = dataset.loc[:, ['speed']]
    phenotype_df = dataset.loc[:, ['dist']]
    covariate_df = pd.DataFrame({})
    expected = pd.DataFrame({'effect': [3.932409], 'standard_error': [0.415513],
        'tvalue': [9.46399], 'pvalue': [1.489836e-12], 'phenotype': ['dist']})
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    assert regression_results_equal(expected, baseline)

def test_r_glm_baseline_covariates():
    dataset = sm.datasets.get_rdataset('cars').data
    genotype_df = dataset.loc[:, ['speed']]
    phenotype_df = dataset.loc[:, ['dist']]
    covariate_df = pd.DataFrame({'intercept': np.ones(genotype_df.shape[0])})
    expected = pd.DataFrame({'effect': [3.932409], 'standard_error': [0.415513],
        'tvalue': [9.46399], 'pvalue': [1.489836e-12], 'phenotype': ['dist']})
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df, fit_intercept=False)
    assert regression_results_equal(expected, baseline)


def test_r_glm():
    dataset = sm.datasets.get_rdataset('cars').data
    genotype_df = dataset.loc[:, ['speed']]
    phenotype_df = dataset.loc[:, ['dist']]
    covariate_df = pd.DataFrame({})
    assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df)

def test_r_glm_covariates():
    dataset = sm.datasets.get_rdataset('cars').data
    genotype_df = dataset.loc[:, ['speed']]
    phenotype_df = dataset.loc[:, ['dist']]
    covariate_df = pd.DataFrame({'intercept': np.ones(genotype_df.shape[0])})
    assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df, fit_intercept=False)

def test_multiple():
    num_samples = 100
    genotype_df = pd.DataFrame(np.random.random((num_samples, 10)))
    phenotype_df = pd.DataFrame(np.random.random((num_samples, 25)))
    covariate_df = pd.DataFrame(np.random.random((num_samples, 5)))
    assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df)

def test_missing():
    num_samples = 5
    genotype_df = pd.DataFrame(np.random.random((num_samples, 1)))
    phenotype_df = pd.DataFrame(np.random.random((num_samples, 1)))
    phenotype_df.loc[0, 0] = np.nan
    covariate_df = pd.DataFrame(np.random.random((num_samples, 1)))
    glow = run_linear_regression(genotype_df, phenotype_df, covariate_df)
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    assert regression_results_equal(glow, baseline)

def test_multiple_spark(spark):
    num_samples = 100
    genotype_df = pd.DataFrame(np.random.random((num_samples, 10)))
    phenotype_df = pd.DataFrame(np.random.random((num_samples, 25)))
    covariate_df = pd.DataFrame(np.random.random((num_samples, 5)))
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    pdf = pd.DataFrame({'values': genotype_df.to_numpy().T.tolist()})
    pdf['idx'] = pdf.index
    results = (lr.linear_regression(spark.createDataFrame(pdf), phenotype_df, covariate_df)
        .toPandas()
        .sort_values(['idx'])
        .drop('idx', axis=1))
    assert regression_results_equal(baseline, results)