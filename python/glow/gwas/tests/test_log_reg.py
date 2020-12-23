import glow.gwas.log_reg as lr
import glow.gwas.functions as gwas_fx
import statsmodels.api as sm
import pandas as pd
import numpy as np
import pytest


def run_score_test(genotype_df, phenotype_df, covariate_df, fit_intercept=True):
    C = covariate_df.to_numpy(copy=True)
    if fit_intercept:
        C = gwas_fx._add_intercept(C, phenotype_df.shape[0])
    Y = phenotype_df.to_numpy(copy=True)
    Y_mask = ~np.isnan(Y)
    Y[~Y_mask] = 0
    state = lr._create_log_reg_state(Y, pd.DataFrame(), None, C, Y_mask, lr.correction_none, fit_intercept)
    values_df = pd.DataFrame({gwas_fx._VALUES_COLUMN_NAME: list(genotype_df.to_numpy().T)})
    return lr._logistic_regression_inner(values_df, state, C, Y_mask, lr.correction_none, 0.05,
                                         phenotype_df.columns.to_series().astype('str'))


def statsmodels_baseline(genotype_df,
                         phenotype_df,
                         covariate_df,
                         offset_dfs=None,
                         fit_intercept=True):
    if fit_intercept:
        covariate_df = sm.add_constant(covariate_df)
    p_values = []
    t_values = []
    for phenotype in phenotype_df:
        for genotype_idx in range(genotype_df.shape[1]):
            mask = ~np.isnan(phenotype_df[phenotype].to_numpy())
            if offset_dfs is not None:
                offset = offset_dfs[genotype_idx][phenotype]
            else:
                offset = None
            model = sm.GLM(phenotype_df[phenotype],
                           covariate_df,
                           offset=offset,
                           family=sm.families.Binomial(),
                           missing='drop')
            params = model.fit().params
            results = model.score_test(params,
                                       exog_extra=genotype_df.iloc[:, genotype_idx].array[mask])
            t_values.append(results[0])
            p_values.append(results[1])
    return pd.DataFrame({
        'tvalue': np.concatenate(t_values),
        'pvalue': np.concatenate(p_values),
        'phenotype': phenotype_df.columns.to_series().astype('str').repeat(genotype_df.shape[1])
    })


def run_logistic_regression_spark(spark,
                                  genotype_df,
                                  phenotype_df,
                                  covariate_df=pd.DataFrame({}),
                                  extra_cols=pd.DataFrame({}),
                                  values_column='values',
                                  **kwargs):
    pdf = pd.DataFrame({values_column: genotype_df.to_numpy().T.tolist()})
    if not extra_cols.empty:
        pdf = pd.concat([pdf, extra_cols], axis=1)
    pdf['idx'] = pdf.index
    results = (lr.logistic_regression(spark.createDataFrame(pdf),
                                      phenotype_df,
                                      covariate_df,
                                      values_column=values_column,
                                      **kwargs).toPandas().sort_values(['phenotype',
                                                                        'idx']).drop('idx', axis=1))
    return results


def regression_results_equal(df1, df2, rtol=1e-5):
    df1 = df1.sort_values('phenotype', kind='mergesort')
    df2 = df2.sort_values('phenotype', kind='mergesort')
    strings_equal = np.array_equal(df1.phenotype.array, df2.phenotype.array)
    numerics_equal = np.allclose(df1.select_dtypes(exclude=['object']),
                                 df2.select_dtypes(exclude=['object']),
                                 rtol=rtol)
    return strings_equal and numerics_equal


def assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df, fit_intercept=True):
    glow = run_score_test(genotype_df, phenotype_df, covariate_df, fit_intercept=fit_intercept)
    golden = statsmodels_baseline(genotype_df,
                                  phenotype_df,
                                  covariate_df,
                                  fit_intercept=fit_intercept)
    assert regression_results_equal(glow, golden)


def random_phenotypes(shape, rg):
    return rg.integers(low=0, high=2, size=shape).astype(np.float64)


def test_spector_non_missing():
    ds = sm.datasets.spector.load_pandas()
    phenotype_df = pd.DataFrame({ds.endog.name: ds.endog})
    genotype_df = ds.exog.loc[:, ['GPA']]
    covariate_df = ds.exog.drop('GPA', axis=1)
    assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df)


def test_spector_missing():
    ds = sm.datasets.spector.load_pandas()
    phenotype_df = pd.DataFrame({ds.endog.name: ds.endog})
    phenotype_df.iloc[[0, 3, 10, 25], 0] = np.nan
    genotype_df = ds.exog.loc[:, ['GPA']]
    covariate_df = ds.exog.drop('GPA', axis=1)
    assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df)


def test_spector_no_intercept():
    ds = sm.datasets.spector.load_pandas()
    phenotype_df = pd.DataFrame({ds.endog.name: ds.endog})
    phenotype_df.iloc[[0, 3, 10, 25], 0] = np.nan
    genotype_df = ds.exog.loc[:, ['GPA']]
    covariate_df = ds.exog.drop('GPA', axis=1)
    assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df, fit_intercept=False)


def test_multiple(rg):
    n_sample = 50
    n_cov = 10
    n_pheno = 25
    phenotype_df = pd.DataFrame(random_phenotypes((n_sample, n_pheno), rg))
    covariate_df = pd.DataFrame(rg.random((n_sample, n_cov)))
    genotype_df = pd.DataFrame(rg.random((n_sample, 1)))
    assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df)


def test_multiple_missing(rg):
    n_sample = 50
    n_cov = 2
    n_pheno = 31
    phenotype_df = pd.DataFrame(random_phenotypes((n_sample, n_pheno), rg))
    Y = phenotype_df.to_numpy()
    Y[np.tril_indices_from(Y, k=-20)] = np.nan
    assert phenotype_df.isna().sum().sum() > 0
    covariate_df = pd.DataFrame(rg.random((n_sample, n_cov)))
    genotype_df = pd.DataFrame(rg.random((n_sample, 1)))
    assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df)


@pytest.mark.min_spark('3')
def test_multiple_spark(spark, rg):
    n_sample = 40
    n_cov = 5
    n_pheno = 5
    phenotype_df = pd.DataFrame(random_phenotypes((n_sample, n_pheno), rg))
    covariate_df = pd.DataFrame(rg.random((n_sample, n_cov)))
    genotype_df = pd.DataFrame(rg.random((n_sample, 1)))
    run_score_test(genotype_df, phenotype_df, covariate_df)
    glow = run_logistic_regression_spark(spark, genotype_df, phenotype_df, covariate_df)
    golden = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    assert regression_results_equal(glow, golden)


def random_mask(size, missing_per_column, rg):
    base = np.ones(size[0], dtype=bool)
    base[:missing_per_column] = False
    return np.column_stack([rg.permutation(base) for _ in range(size[1])])


@pytest.mark.min_spark('3')
def test_multiple_spark_missing(spark, rg):
    n_sample = 50
    n_cov = 5
    n_pheno = 5
    phenotype_df = pd.DataFrame(random_phenotypes((n_sample, n_pheno), rg))
    Y = phenotype_df.to_numpy()
    Y[~random_mask(Y.shape, 10, rg)] = np.nan
    assert phenotype_df.isna().sum().sum() > 0
    covariate_df = pd.DataFrame(rg.random((n_sample, n_cov)))
    genotype_df = pd.DataFrame(rg.random((n_sample, 1)))
    glow = run_logistic_regression_spark(spark, genotype_df, phenotype_df, covariate_df)
    golden = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    assert regression_results_equal(glow, golden)


@pytest.mark.min_spark('3')
def test_spark_no_intercept(spark, rg):
    n_sample = 50
    n_cov = 5
    n_pheno = 5
    phenotype_df = pd.DataFrame(random_phenotypes((n_sample, n_pheno), rg))
    Y = phenotype_df.to_numpy()
    Y[~random_mask(Y.shape, 15, rg)] = np.nan
    assert phenotype_df.isna().sum().sum() > 0
    covariate_df = pd.DataFrame(rg.random((n_sample, n_cov)))
    genotype_df = pd.DataFrame(rg.random((n_sample, 1)))
    glow = run_logistic_regression_spark(spark,
                                         genotype_df,
                                         phenotype_df,
                                         covariate_df,
                                         fit_intercept=False)
    golden = statsmodels_baseline(genotype_df, phenotype_df, covariate_df, fit_intercept=False)
    assert regression_results_equal(glow, golden)


@pytest.mark.min_spark('3')
def test_simple_offset(spark, rg):
    num_samples = 25
    num_pheno = 6
    num_geno = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, num_geno)))
    phenotype_df = pd.DataFrame(random_phenotypes((num_samples, num_pheno), rg))
    covariate_df = pd.DataFrame(rg.random((num_samples, 2)))
    offset_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    results = run_logistic_regression_spark(spark,
                                            genotype_df,
                                            phenotype_df,
                                            covariate_df,
                                            offset_df=offset_df)
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df, [offset_df] * num_geno)
    assert regression_results_equal(results, baseline)


@pytest.mark.min_spark('3')
def test_multi_offset(spark, rg):
    num_samples = 50
    num_pheno = 25
    num_geno = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, num_geno)))
    phenotype_df = pd.DataFrame(random_phenotypes((num_samples, num_pheno), rg))
    covariate_df = pd.DataFrame(rg.random((num_samples, 10)))
    offset_index = pd.MultiIndex.from_product([phenotype_df.index, ['chr1', 'chr2']])
    offset_df = pd.DataFrame(rg.random((num_samples * 2, num_pheno)), index=offset_index)
    extra_cols = pd.DataFrame({'contigName': ['chr1', 'chr2'] * 5})
    results = run_logistic_regression_spark(spark,
                                            genotype_df,
                                            phenotype_df,
                                            covariate_df,
                                            offset_df=offset_df,
                                            extra_cols=extra_cols)
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df,
                                    [offset_df.xs('chr1', level=1),
                                     offset_df.xs('chr2', level=1)] * 5)
    assert regression_results_equal(results, baseline)


@pytest.mark.min_spark('3')
def test_cast_genotypes_float32(spark, rg):
    num_samples = 50
    genotype_df = pd.DataFrame(rg.integers(0, 10, (num_samples, 10)))
    phenotype_df = pd.DataFrame(random_phenotypes((num_samples, 5), rg))
    covariate_df = pd.DataFrame(rg.random((num_samples, 5)))
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    results = run_logistic_regression_spark(spark,
                                            genotype_df,
                                            phenotype_df,
                                            covariate_df,
                                            dt=np.float32)
    assert results['pvalue'].dtype == np.float32
    assert regression_results_equal(baseline, results, rtol=1e-4)  # Higher rtol for float32


@pytest.mark.min_spark('3')
def test_multi_offset_with_missing(spark, rg):
    num_samples = 25
    num_pheno = 24
    num_geno = 18
    contigs = ['chr1', 'chr2', 'chr3']
    genotype_df = pd.DataFrame(rg.random((num_samples, num_geno)))
    phenotype_df = pd.DataFrame(random_phenotypes((num_samples, num_pheno), rg))
    phenotype_df.iloc[0, 0] = np.nan
    phenotype_df.iloc[1, 0] = np.nan
    covariate_df = pd.DataFrame(rg.random((num_samples, 2)))
    offset_index = pd.MultiIndex.from_product([phenotype_df.index, contigs])
    offset_df = pd.DataFrame(rg.random((num_samples * len(contigs), num_pheno)), index=offset_index)
    extra_cols = pd.DataFrame({'contigName': contigs * 6})
    results = run_logistic_regression_spark(spark,
                                            genotype_df,
                                            phenotype_df,
                                            covariate_df,
                                            offset_df=offset_df,
                                            extra_cols=extra_cols)
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df,
                                    [offset_df.xs(contig, level=1) for contig in contigs] * 6)
    assert regression_results_equal(results, baseline)


def test_error_for_old_spark(spark, rg):
    if spark.version.startswith('2'):
        num_samples = 10
        genotype_df = pd.DataFrame(rg.random((num_samples, 10)))
        phenotype_df = pd.DataFrame(random_phenotypes((num_samples, 25), rg))
        with pytest.raises(AttributeError):
            run_logistic_regression_spark(spark, genotype_df, phenotype_df, pd.DataFrame({}))


@pytest.mark.min_spark('3')
def test_intercept_no_covariates(spark, rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 10)))
    phenotype_df = pd.DataFrame(random_phenotypes((num_samples, 2), rg))
    # No error
    run_logistic_regression_spark(spark, genotype_df, phenotype_df, pd.DataFrame({}))


@pytest.mark.min_spark('3')
def test_propagate_extra_cols(spark, rg):
    num_samples = 50
    genotype_df = pd.DataFrame(rg.random((num_samples, 3)))
    phenotype_df = pd.DataFrame(random_phenotypes((num_samples, 5), rg))
    covariate_df = pd.DataFrame(rg.random((num_samples, 2)))
    extra_cols = pd.DataFrame({'genotype_idx': range(3), 'animal': 'monkey'})
    results = run_logistic_regression_spark(spark, genotype_df, phenotype_df, covariate_df,
                                            extra_cols)
    assert sorted(results['genotype_idx'].tolist()) == [0] * 5 + [1] * 5 + [2] * 5
    assert results.animal[results.animal == 'monkey'].all()
    assert results.columns.tolist() == ['genotype_idx', 'animal', 'tvalue', 'pvalue', 'phenotype']
