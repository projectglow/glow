import functions as fx
import numpy as np
import statsmodels.api as sm
import pandas as pd
import glow.gwas.functions as gwas_fx
import glow.gwas.lin_reg as lr
import pytest


def run_linear_regression(genotype_df, phenotype_df, covariate_df, add_intercept=True):
    phenotype_names = phenotype_df.columns.astype('str').to_series()
    C = covariate_df.to_numpy('float64', copy=True)
    if add_intercept:
        C = gwas_fx._add_intercept(C, genotype_df.shape[0])
    if not C.size:
        C = np.zeros((genotype_df.shape[0], 1))
    Y = phenotype_df.to_numpy('float64', copy=True)
    Y_mask = ~np.isnan(Y)
    Y[~Y_mask] = 0
    Y -= Y.mean(axis=0)
    Q = np.linalg.qr(C)[0]
    Y = gwas_fx._residualize_in_place(Y, Q) * Y_mask
    Y_scale = np.ones(Y.shape[1])
    Y_state = lr._create_YState(Y, phenotype_df, pd.DataFrame({}), Y_mask, np.float64, None)
    dof = C.shape[0] - C.shape[1] - 1
    pdf = pd.DataFrame({lr._VALUES_COLUMN_NAME: list(genotype_df.to_numpy('float64').T)})

    return lr._linear_regression_inner(pdf, Y_state, Y_mask.astype('float64'), Y_scale, Q, dof,
                                       phenotype_names, None, None)


def run_linear_regression_spark(spark,
                                genotype_df,
                                phenotype_df,
                                covariate_df=pd.DataFrame({}),
                                extra_cols=pd.DataFrame({}),
                                offset_df=pd.DataFrame({}),
                                values_column='values',
                                **kwargs):
    pdf = pd.DataFrame({values_column: genotype_df.to_numpy().T.tolist()})
    if not extra_cols.empty:
        pdf = pd.concat([pdf, extra_cols], axis=1)
    pdf['idx'] = pdf.index
    results = lr.linear_regression(spark.createDataFrame(pdf),
                                   phenotype_df,
                                   covariate_df,
                                   offset_df,
                                   values_column=values_column,
                                   **kwargs).toPandas().sort_values(['phenotype',
                                                                     'idx']).drop('idx', axis=1)
    return results


def residualize(Y, X):
    filled = np.nan_to_num(Y)
    coef = np.linalg.lstsq(X, filled, rcond=None)[0]
    pred = X @ coef
    return Y - pred


def statsmodels_baseline(genotype_df,
                         phenotype_df,
                         covariate_df,
                         offset_dfs=None,
                         add_intercept=True):
    # Project out covariates from genotypes and phenotypes
    C = covariate_df.to_numpy('float64')
    num_samples = C.shape[0] if C.size else genotype_df.shape[0]
    if add_intercept:
        C = gwas_fx._add_intercept(C, num_samples)
    Y = phenotype_df.to_numpy('float64')
    X = genotype_df.to_numpy('float64')
    phenotype_df.columns = phenotype_df.columns.astype('str')
    dof = C.shape[0] - C.shape[1] - 1
    effects = []
    errors = []
    tvalues = []
    pvalues = []
    for phenotype_idx in range(Y.shape[1]):
        for genotype_idx in range(X.shape[1]):
            phenotype = Y[:, phenotype_idx].copy()
            phenotype_mask = ~np.isnan(phenotype)
            phenotype[~phenotype_mask] = 0
            phenotype -= phenotype.mean()
            phenotype = residualize(phenotype, C) * phenotype_mask
            phenotype_scale = np.sqrt((phenotype**2).sum() / (phenotype_mask.sum() - C.shape[1]))
            phenotype /= phenotype_scale
            if offset_dfs:
                offset = offset_dfs[genotype_idx].iloc[:, phenotype_idx].to_numpy('float64')
                phenotype = phenotype - offset
            phenotype[~phenotype_mask] = np.nan

            genotype = residualize(X[:, genotype_idx], C)
            genotype = pd.Series(genotype, name='genotype')

            model = sm.OLS(phenotype, genotype, missing='drop')
            model.df_resid = dof
            results = model.fit()

            effects.append(results.params.genotype * phenotype_scale)
            errors.append(results.bse.genotype * phenotype_scale)
            tvalues.append(results.tvalues.genotype)
            pvalues.append(results.pvalues.genotype)
    return pd.DataFrame({
        'effect': effects,
        'standard_error': errors,
        'tvalue': tvalues,
        'pvalue': pvalues,
        'phenotype': phenotype_df.columns.to_series().repeat(genotype_df.shape[1])
    })


def regression_results_equal(df1, df2, rtol=1e-5):
    df1 = df1.sort_values('phenotype', kind='mergesort')
    df2 = df2.sort_values('phenotype', kind='mergesort')

    strings_equal = np.array_equal(df1.phenotype.array, df2.phenotype.array)
    numerics_equal = np.allclose(df1.select_dtypes(exclude=['object']),
                                 df2.select_dtypes(exclude=['object']),
                                 rtol=rtol)
    return strings_equal and numerics_equal


def assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df, add_intercept=True):
    glow = run_linear_regression(genotype_df,
                                 phenotype_df,
                                 covariate_df,
                                 add_intercept=add_intercept)
    golden = statsmodels_baseline(genotype_df,
                                  phenotype_df,
                                  covariate_df,
                                  add_intercept=add_intercept)
    assert regression_results_equal(glow, golden)


def test_r_glm_baseline():
    dataset = sm.datasets.get_rdataset('cars').data
    genotype_df = dataset.loc[:, ['speed']]
    phenotype_df = dataset.loc[:, ['dist']]
    covariate_df = pd.DataFrame({})
    expected = pd.DataFrame({
        'effect': [3.932409],
        'standard_error': [0.415513],
        'tvalue': [9.46399],
        'pvalue': [1.489836e-12],
        'phenotype': ['dist']
    })
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    assert regression_results_equal(expected, baseline)


def test_r_glm_baseline_covariates():
    dataset = sm.datasets.get_rdataset('cars').data
    genotype_df = dataset.loc[:, ['speed']]
    phenotype_df = dataset.loc[:, ['dist']]
    covariate_df = pd.DataFrame({'intercept': np.ones(genotype_df.shape[0])})
    expected = pd.DataFrame({
        'effect': [3.932409],
        'standard_error': [0.415513],
        'tvalue': [9.46399],
        'pvalue': [1.489836e-12],
        'phenotype': ['dist']
    })
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df, add_intercept=False)
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
    phenotype_df.loc[0, 'dist'] = np.nan
    covariate_df = pd.DataFrame({'intercept': np.ones(genotype_df.shape[0])})
    assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df, add_intercept=False)


def test_multiple(rg):
    num_samples = 100
    genotype_df = pd.DataFrame(rg.random((num_samples, 10)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 25)))
    covariate_df = pd.DataFrame(rg.random((num_samples, 5)))
    assert_glow_equals_golden(genotype_df, phenotype_df, covariate_df)


def test_missing(rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 1)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 1)))
    phenotype_df.loc[0, 0] = np.nan
    covariate_df = pd.DataFrame(rg.random((num_samples, 3)))
    glow = run_linear_regression(genotype_df, phenotype_df, covariate_df)
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    assert regression_results_equal(glow, baseline)


@pytest.mark.min_spark('3')
def test_missing_spark(spark, rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 1)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 3)))
    phenotype_df.loc[0, 0] = np.nan
    phenotype_df.loc[[1, 3, 5], 1] = np.nan
    covariate_df = pd.DataFrame(rg.random((num_samples, 3)))
    glow = run_linear_regression_spark(spark, genotype_df, phenotype_df, covariate_df)
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    assert regression_results_equal(glow, baseline)


@pytest.mark.min_spark('3')
def test_verbose_output(spark, rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 1)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 3)))
    phenotype_df.loc[0, 0] = np.nan
    phenotype_df.loc[[1, 3, 5], 1] = np.nan
    covariate_df = pd.DataFrame(rg.random((num_samples, 3)))
    glow = run_linear_regression_spark(spark, genotype_df, phenotype_df, covariate_df, verbose_output = True)
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    assert regression_results_equal(glow.drop(columns=["n","sum_x","y_transpose_x"]), baseline)
    assert glow.n.to_list() == [9, 7, 10]
    assert np.allclose(glow.y_transpose_x.to_numpy(), np.nan_to_num(phenotype_df.to_numpy().T) @ genotype_df[0].array)
    assert np.allclose(glow.sum_x.to_numpy(), (~np.isnan(phenotype_df.to_numpy())).T @ genotype_df[0].array)

@pytest.mark.min_spark('3')
def test_verbose_output(spark, rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 1)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 3)))
    phenotype_df.loc[0, 0] = np.nan
    phenotype_df.loc[[1, 3, 5], 1] = np.nan
    covariate_df = pd.DataFrame(rg.random((num_samples, 3)))
    glow = run_linear_regression_spark(spark,
                                       genotype_df,
                                       phenotype_df,
                                       covariate_df,
                                       verbose_output=True)
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    assert regression_results_equal(glow.drop(columns=["n", "sum_x", "y_transpose_x"]), baseline)
    assert glow.n.to_list() == [9, 7, 10]
    assert np.allclose(glow.y_transpose_x.to_numpy(),
                       np.nan_to_num(phenotype_df.to_numpy().T) @ genotype_df[0].array)
    assert np.allclose(glow.sum_x.to_numpy(),
                       (~np.isnan(phenotype_df.to_numpy())).T @ genotype_df[0].array)


@pytest.mark.min_spark('3')
def test_multiple_spark(spark, rg):
    num_samples = 100
    genotype_df = pd.DataFrame(rg.random((num_samples, 10)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 25)) * 10)
    covariate_df = pd.DataFrame(rg.random((num_samples, 5)))
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    results = run_linear_regression_spark(spark, genotype_df, phenotype_df, covariate_df)
    assert regression_results_equal(baseline, results)


@pytest.mark.min_spark('3')
def test_propagate_extra_cols(spark, rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 3)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 5)))
    covariate_df = pd.DataFrame(rg.random((num_samples, 2)))
    extra_cols = pd.DataFrame({'genotype_idx': range(3), 'animal': 'monkey'})
    results = run_linear_regression_spark(spark, genotype_df, phenotype_df, covariate_df,
                                          extra_cols)
    assert sorted(results['genotype_idx'].tolist()) == [0] * 5 + [1] * 5 + [2] * 5
    assert results.animal[results.animal == 'monkey'].all()
    assert results.columns.tolist() == [
        'genotype_idx', 'animal', 'effect', 'stderror', 'tvalue', 'pvalue', 'phenotype'
    ]


@pytest.mark.min_spark('3')
def test_different_values_column(spark, rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 3)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 5)))
    covariate_df = pd.DataFrame(rg.random((num_samples, 2)))
    results = run_linear_regression_spark(spark,
                                          genotype_df,
                                          phenotype_df,
                                          covariate_df,
                                          values_column='numbers')
    assert results.columns.tolist() == ['effect', 'stderror', 'tvalue', 'pvalue', 'phenotype']


@pytest.mark.min_spark('3')
def test_intercept_no_covariates(spark, rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 10)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 25)))
    # No error
    run_linear_regression_spark(spark, genotype_df, phenotype_df, pd.DataFrame({}))


@pytest.mark.min_spark('3')
def test_validates_missing_covariates(spark, rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 3)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 5)))
    covariate_df = pd.DataFrame(rg.random((num_samples, 2)))
    covariate_df.loc[0, 0] = np.nan
    with pytest.raises(ValueError):
        run_linear_regression_spark(spark, genotype_df, phenotype_df, covariate_df)


@pytest.mark.min_spark('3')
def test_validate_same_number_of_rows(spark, rg):
    genotype_df = pd.DataFrame(rg.random((4, 3)))
    phenotype_df = pd.DataFrame(rg.random((4, 5)))
    covariate_df = pd.DataFrame(rg.random((5, 2)))
    with pytest.raises(ValueError):
        run_linear_regression_spark(spark, genotype_df, phenotype_df, covariate_df)


def test_error_for_old_spark(spark, rg):
    if spark.version.startswith('2'):
        num_samples = 10
        genotype_df = pd.DataFrame(rg.random((num_samples, 10)))
        phenotype_df = pd.DataFrame(rg.random((num_samples, 25)))
        with pytest.raises(AttributeError):
            run_linear_regression_spark(spark, genotype_df, phenotype_df, pd.DataFrame({}))


@pytest.mark.min_spark('3')
def test_simple_offset(spark, rg):
    num_samples = 25
    num_pheno = 6
    num_geno = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, num_geno)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    covariate_df = pd.DataFrame(rg.random((num_samples, 2)))
    offset_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    results = run_linear_regression_spark(spark,
                                          genotype_df,
                                          phenotype_df,
                                          covariate_df,
                                          offset_df=offset_df)
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df, [offset_df] * num_geno)
    assert regression_results_equal(results, baseline)


@pytest.mark.min_spark('3')
def test_simple_offset_out_of_order(spark, rg):
    num_samples = 25
    num_pheno = 6
    num_geno = 10
    samples = [f'sample_{i}' for i in range(0, num_samples)]
    traits = [f'trait_{i}' for i in range(0, num_pheno)]
    genos = [f'snp_{i}' for i in range(0, num_geno)]
    genotype_df = pd.DataFrame(rg.random((num_samples, num_geno)), samples, genos)
    phenotype_df = pd.DataFrame(rg.random((num_samples, num_pheno)), samples, traits)
    covariate_df = pd.DataFrame(rg.random((num_samples, 2)))
    offset_df = pd.DataFrame(rg.random((num_samples, num_pheno)), samples, traits)
    results = run_linear_regression_spark(spark,
                                          genotype_df,
                                          phenotype_df,
                                          covariate_df,
                                          offset_df=offset_df.sample(frac=1))
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df, [offset_df] * num_geno)
    assert regression_results_equal(results, baseline)


@pytest.mark.min_spark('3')
def test_multi_offset(spark, rg):
    num_samples = 25
    num_pheno = 25
    num_geno = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, num_geno)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    covariate_df = pd.DataFrame(rg.random((num_samples, 10)))
    offset_index = pd.MultiIndex.from_product([phenotype_df.index, ['chr1', 'chr2']])
    offset_df = pd.DataFrame(rg.random((num_samples * 2, num_pheno)), index=offset_index)
    extra_cols = pd.DataFrame({'contigName': ['chr1', 'chr2'] * 5})
    results = run_linear_regression_spark(spark,
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
def test_multi_offset_with_missing(spark, rg):
    num_samples = 25
    num_pheno = 24
    num_geno = 18
    contigs = ['chr1', 'chr2', 'chr3']
    genotype_df = pd.DataFrame(rg.random((num_samples, num_geno)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    missing = np.zeros(phenotype_df.shape)
    np.fill_diagonal(missing, 1)
    missing[:, -1] = 0
    phenotype_df[missing.astype('bool')] = np.nan
    covariate_df = pd.DataFrame(rg.random((num_samples, 10)))
    offset_index = pd.MultiIndex.from_product([phenotype_df.index, contigs])
    offset_df = pd.DataFrame(rg.random((num_samples * len(contigs), num_pheno)), index=offset_index)
    extra_cols = pd.DataFrame({'contigName': contigs * 6})
    results = run_linear_regression_spark(spark,
                                          genotype_df,
                                          phenotype_df,
                                          covariate_df,
                                          offset_df=offset_df,
                                          extra_cols=extra_cols)
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df,
                                    [offset_df.xs(contig, level=1) for contig in contigs] * 6)

    assert regression_results_equal(results, baseline)


@pytest.mark.min_spark('3')
def test_too_many_missing(spark, rg):
    num_samples = 25
    num_pheno = 24
    num_geno = 18
    contigs = ['chr1', 'chr2', 'chr3']
    genotype_df = pd.DataFrame(rg.random((num_samples, num_geno)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    missing = np.triu(np.ones(phenotype_df.shape))
    missing[:, -1] = 0
    phenotype_df[missing.astype('bool')] = np.nan
    covariate_df = pd.DataFrame(rg.random((num_samples, 10)))
    offset_index = pd.MultiIndex.from_product([phenotype_df.index, contigs])
    offset_df = pd.DataFrame(rg.random((num_samples * len(contigs), num_pheno)), index=offset_index)
    extra_cols = pd.DataFrame({'contigName': contigs * 6})
    with pytest.raises(ValueError):
        run_linear_regression_spark(spark,
                                    genotype_df,
                                    phenotype_df,
                                    covariate_df,
                                    offset_df=offset_df,
                                    extra_cols=extra_cols)


@pytest.mark.min_spark('3')
def test_offset_wrong_columns(spark, rg):
    num_samples = 25
    num_pheno = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 10)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    offset_df = pd.DataFrame(rg.random((num_samples, num_pheno)), columns=range(10, 20))
    with pytest.raises(ValueError):
        run_linear_regression_spark(spark, genotype_df, phenotype_df, offset_df=offset_df)


@pytest.mark.min_spark('3')
def test_offset_wrong_index(spark, rg):
    num_samples = 25
    num_pheno = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 10)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    offset_df = pd.DataFrame(rg.random((num_samples, num_pheno)), index=range(1, 26))
    with pytest.raises(ValueError):
        run_linear_regression_spark(spark, genotype_df, phenotype_df, offset_df=offset_df)


@pytest.mark.min_spark('3')
def test_offset_wrong_multi_index(spark, rg):
    num_samples = 25
    num_pheno = 10
    contigs = ['chr1', 'chr2']
    genotype_df = pd.DataFrame(rg.random((num_samples, 10)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    offset_df = pd.DataFrame(rg.random((num_samples * len(contigs), num_pheno)),
                             pd.MultiIndex.from_product([range(1, 26), contigs]))
    with pytest.raises(ValueError):
        run_linear_regression_spark(spark, genotype_df, phenotype_df, offset_df=offset_df)


@pytest.mark.min_spark('3')
def test_offset_different_index_order(spark, rg):
    num_samples = 25
    num_pheno = 6
    num_geno = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, num_geno)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    phenotype_df.columns = phenotype_df.columns.astype('str')
    covariate_df = pd.DataFrame(rg.random((num_samples, 2)))
    offset_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    offset_df.columns = offset_df.columns.astype('str')
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df, [offset_df] * num_geno)

    # Results should not depend on index order
    offset_df = offset_df.sort_index(axis=0, ascending=False)
    offset_df = offset_df.sort_index(axis=1, ascending=False)

    results = run_linear_regression_spark(spark,
                                          genotype_df,
                                          phenotype_df,
                                          covariate_df,
                                          offset_df=offset_df)
    assert regression_results_equal(results, baseline)


@pytest.mark.min_spark('3')
def test_cast_genotypes(spark, rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.integers(0, 10, (num_samples, 10)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 5)))
    covariate_df = pd.DataFrame(rg.random((num_samples, 5)))
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    results = run_linear_regression_spark(spark, genotype_df, phenotype_df, covariate_df)
    assert results['effect'].dtype == np.float64
    assert regression_results_equal(baseline, results)


@pytest.mark.min_spark('3')
def test_cast_genotypes_float32(spark, rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.integers(0, 10, (num_samples, 10)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 5)))
    covariate_df = pd.DataFrame(rg.random((num_samples, 5)))
    baseline = statsmodels_baseline(genotype_df, phenotype_df, covariate_df)
    results = run_linear_regression_spark(spark,
                                          genotype_df,
                                          phenotype_df,
                                          covariate_df,
                                          dt=np.float32)
    assert results['effect'].dtype == np.float32
    assert regression_results_equal(baseline, results, rtol=1e-4)  # Higher rtol for float32


@pytest.mark.min_spark('3')
def test_bad_datatype(spark, rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 10)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 5)))
    covariate_df = pd.DataFrame(rg.random((num_samples, 5)))
    with pytest.raises(ValueError):
        run_linear_regression_spark(spark, genotype_df, phenotype_df, covariate_df, dt=np.int32)


@pytest.mark.min_spark('3')
def test_bad_column_name(spark, rg):
    num_samples = 10
    genotype_df = pd.DataFrame(rg.random((num_samples, 10)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 5)))
    covariate_df = pd.DataFrame(rg.random((num_samples, 5)))
    with pytest.raises(ValueError):
        run_linear_regression_spark(spark,
                                    genotype_df,
                                    phenotype_df,
                                    covariate_df,
                                    values_column='genotypes')


@pytest.mark.min_spark('3')
def test_values_expr(spark, rg):
    from pyspark.sql.functions import array, lit
    num_samples = 5
    genotype_df = spark.range(1).withColumn('genotypes', lit(42))
    phenotype_df = pd.DataFrame(rg.random((num_samples, 5)))
    covariate_df = pd.DataFrame(rg.random((num_samples, 2)))
    array_vals = [lit(i) for i in range(num_samples)]
    results = lr.linear_regression(genotype_df,
                                   phenotype_df,
                                   covariate_df,
                                   values_column=array(*array_vals))
    pandas_genotype_df = pd.DataFrame({'values': range(num_samples)})
    baseline = statsmodels_baseline(pandas_genotype_df, phenotype_df, covariate_df)
    assert regression_results_equal(baseline, results.drop('id').toPandas())
    assert not 'genotypes' in [field.name for field in results.schema]


def test_subset_contigs(rg):
    num_samples = 25
    num_pheno = 25
    contigs = ['chr1', 'chr2', 'chr3']
    phenotype_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    offset_index = pd.MultiIndex.from_product([phenotype_df.index, contigs])
    offset_df = pd.DataFrame(rg.random((num_samples * 3, num_pheno)), index=offset_index)
    state = lr._create_YState(phenotype_df.to_numpy(), phenotype_df, offset_df,
                              ~np.isnan(phenotype_df.to_numpy()), np.float64, None)
    assert set(state.keys()) == set(contigs)
    state = lr._create_YState(phenotype_df.to_numpy(), phenotype_df, offset_df,
                              ~np.isnan(phenotype_df.to_numpy()), np.float64, ['chr1', 'chr3'])
    assert set(state.keys()) == set(['chr1', 'chr3'])


@pytest.mark.min_spark('3')
def test_subset_contigs_no_loco(spark, rg):
    num_samples = 50
    num_pheno = 5
    genotype_df = pd.DataFrame(rg.random((num_samples, 3)))
    phenotype_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    offset_df = pd.DataFrame(rg.random((num_samples, num_pheno)))
    # No error when contigs are provided without loco offsets
    run_linear_regression_spark(spark,
                                genotype_df,
                                phenotype_df,
                                offset_df=offset_df,
                                contigs=['chr1'])


def compare_linreg_to_regenie(spark, output_prefix, missing=[], single_chr=True):

    (genotype_df, phenotype_df, covariate_df, offset_df) = fx.get_input_dfs(spark,
                                                                            binary=False,
                                                                            missing=missing,
                                                                            single_chr=single_chr)
    glowgr_df = lr.linear_regression(genotype_df,
                                     phenotype_df,
                                     covariate_df,
                                     offset_df,
                                     values_column='values').toPandas()
    fx.compare_to_regenie(output_prefix, glowgr_df)


@pytest.mark.min_spark('3')
def test_versus_regenie(spark):
    compare_linreg_to_regenie(spark, 'test_lin_out_')


@pytest.mark.min_spark('3')
def test_missing_versus_regenie(spark):
    compare_linreg_to_regenie(
        spark,
        'test_lin_out_missing_',
        missing=['35_35', '136_136', '77_77', '100_100', '204_204', '474_474'])


@pytest.mark.min_spark('3')
def test_three_chr_versus_regenie(spark):
    compare_linreg_to_regenie(spark, 'test_lin_out_3chr_', single_chr=False)
