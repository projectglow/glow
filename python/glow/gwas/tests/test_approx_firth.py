from dataclasses import dataclass
import glow.gwas.log_reg as lr
import glow.gwas.approx_firth as af
import glow.functions as fx
import pandas as pd
from pandas.testing import assert_frame_equal
from nptyping import Float, NDArray
import numpy as np
import pytest
from typing import Any


@dataclass
class TestData:
    phenotypes: NDArray[(Any, ), Float]
    covariates: NDArray[(Any, Any), Float]
    offset: NDArray[(Any, ), Float]


def _get_test_data(use_offset, use_intercept):
    test_file = 'test-data/r/sex2withoffset.txt'
    df = pd.read_table(test_file, delimiter='\t').astype('float64')
    phenotypes = df['case']
    covariates = df.loc[:, 'age':'dia']
    if use_intercept:
        covariates.loc[:, 'intercept'] = 1
    offset = df['offset']
    if not use_offset:
        offset = offset * 0
    return TestData(phenotypes.to_numpy(), covariates.to_numpy(), offset.to_numpy())


def _compare_full_firth_beta(test_data, golden_firth_beta):
    beta_init = np.zeros(test_data.covariates.shape[1])
    X = test_data.covariates
    y = test_data.phenotypes
    offset = test_data.offset

    test_firth_fit = af._fit_firth(beta_init=beta_init, X=X, y=y, offset=offset)
    test_firth_beta = test_firth_fit.beta
    assert np.allclose(golden_firth_beta, test_firth_beta)


def test_full_firth():
    # table = read.table("sex2withoffset.txt", header=True)
    # logistf(case ~ age+oc+vic+vicl+vis+dia+offset(offset), data=table)
    golden_firth_beta = [
        -1.1715911,  # age
        0.1568537,  # oc
        2.4752617,  # vic
        -2.2125007,  # vicl
        -0.8604622,  # vis
        2.7397140,  # dia
        -0.5679234  # intercept
    ]
    test_data = _get_test_data(use_offset=True, use_intercept=True)
    _compare_full_firth_beta(test_data, golden_firth_beta)


def test_full_firth_no_offset():
    # logistf(case ~ age+oc+vic+vicl+vis+dia, data=table)
    golden_firth_beta = [
        -1.10598130,  # age
        -0.06881673,  # oc
        2.26887464,  # vic
        -2.11140816,  # vicl
        -0.78831694,  # vis
        3.09601263,  # dia
        0.12025404  # intercept
    ]
    test_data = _get_test_data(use_offset=False, use_intercept=True)
    _compare_full_firth_beta(test_data, golden_firth_beta)


def test_full_firth_no_intercept():
    # logistf(case ~ age+oc+vic+vicl+vis+dia+offset(offset)-1, data=table)
    golden_firth_beta = [
        -1.2513849,  # age
        -0.3141151,  # oc
        2.2066573,  # vic
        -2.2988439,  # vicl
        -0.9922712,  # vis
        2.7046574  # dia
    ]
    test_data = _get_test_data(use_offset=True, use_intercept=False)
    _compare_full_firth_beta(test_data, golden_firth_beta)


def test_null_firth_fit_no_offset():
    golden_firth_beta = [
        -1.10598130,  # age
        -0.06881673,  # oc
        2.26887464,  # vic
        -2.11140816,  # vicl
        -0.78831694,  # vis
        3.09601263,  # dia
        0.12025404  # intercept
    ]
    test_data = _get_test_data(use_offset=False, use_intercept=True)
    fit = af.perform_null_firth_fit(test_data.phenotypes,
                                    test_data.covariates,
                                    ~np.isnan(test_data.phenotypes),
                                    None,
                                    includes_intercept=True)
    assert np.allclose(fit, test_data.covariates @ golden_firth_beta)


def test_profile_big(rg):
    n = 5000000
    y = rg.integers(low=0, high=2, size=n).astype(np.float64)
    C = np.random.random((n, 10))
    C[:, 0] = 1
    mask = ~np.isnan(y)
    import cProfile
    cProfile.runctx('af.perform_null_firth_fit(y, C, mask, None, True)',
                    globals(),
                    locals(),
                    sort='cumtime')


def _set_fid_iid_df(df):
    df['FID_IID'] = df['FID'].astype(str) + '_' + df['IID'].astype(str)
    return df.sort_values(by=['FID', 'IID']) \
        .drop(columns=['FID', 'IID']) \
        .set_index(['FID_IID'])


def _read_offset_df(file, trait):
    df = pd.melt(pd.read_table(file, sep=r'\s+'), id_vars=['FID_IID']) \
        .rename(columns={'FID_IID': 'contigName', 'variable': 'FID_IID', 'value': trait}) \
        .astype({'FID_IID': 'str', 'contigName': 'str'})
    df[['FID', 'IID']] = df.FID_IID.str.split('_', expand=True)
    return df.sort_values(by=['FID', 'IID']) \
        .drop(columns=['FID', 'IID']) \
        .set_index(['FID_IID', 'contigName'])


def _read_regenie_df(file, trait, num_snps):
    df = pd.read_table(file, sep=r'\s+')
    df = df[df['ID'] <= num_snps]
    df['phenotype'] = trait
    return df


def compare_to_regenie(spark,
                       pvalue_threshold,
                       regenie_prefix,
                       compare_all_cols,
                       uncorrected,
                       corrected,
                       missing=[]):
    test_data_dir = 'test-data/regenie/'

    num_snps = 100  # Spot check

    genotype_df = spark.read.format('bgen').load(test_data_dir + 'example.bgen') \
        .withColumn('values', fx.genotype_states('genotypes')) \
        .filter(f'start < {num_snps}')

    phenotype_df = _set_fid_iid_df(pd.read_table(test_data_dir + 'phenotype_bin.txt', sep=r'\s+'))
    phenotype_df.loc[missing, :] = np.nan

    covariate_df = _set_fid_iid_df(pd.read_table(test_data_dir + 'covariates.txt', sep=r'\s+'))

    offset_trait1_df = _read_offset_df(test_data_dir + 'fit_bin_out_1.loco', 'Y1')
    offset_trait2_df = _read_offset_df(test_data_dir + 'fit_bin_out_2.loco', 'Y2')
    offset_df = pd.merge(offset_trait1_df, offset_trait2_df, left_index=True, right_index=True)

    glowgr_df = lr.logistic_regression(genotype_df,
                                       phenotype_df,
                                       covariate_df,
                                       offset_df,
                                       correction=lr.correction_approx_firth,
                                       pvalue_threshold=pvalue_threshold,
                                       values_column='values').toPandas()

    regenie_files = [
        test_data_dir + regenie_prefix + 'Y1.regenie', test_data_dir + regenie_prefix + 'Y2.regenie'
    ]
    regenie_traits = ['Y1', 'Y2']
    regenie_df = pd.concat(
        [_read_regenie_df(f, t, num_snps) for f, t in zip(regenie_files, regenie_traits)],
        ignore_index=True)

    glowgr_df['ID'] = glowgr_df['names'].apply(lambda x: int(x[-1]))
    glowgr_df = glowgr_df.rename(columns={
        'effect': 'BETA',
        'stderror': 'SE'
    }).astype({'ID': 'int64'})
    regenie_df['pvalue'] = np.power(10, -regenie_df['LOG10P'])

    if compare_all_cols:
        cols = ['ID', 'BETA', 'SE', 'pvalue', 'phenotype']
    else:
        cols = ['ID', 'pvalue', 'phenotype']
    assert_frame_equal(glowgr_df[cols], regenie_df[cols], check_dtype=False, check_less_precise=1)

    correction_counts = glowgr_df.correction_succeeded.value_counts(dropna=False).to_dict()
    if uncorrected > 0:
        # null in Spark DataFrame converts to nan in pandas
        assert correction_counts[np.nan] == uncorrected
    if corrected > 0:
        assert correction_counts[True] == corrected
    assert False not in correction_counts

    return glowgr_df


@pytest.mark.min_spark('3')
def test_correct_all_versus_regenie(spark):
    compare_to_regenie(spark,
                       0.9999,
                       'test_bin_out_firth_',
                       compare_all_cols=True,
                       uncorrected=0,
                       corrected=200)


@pytest.mark.min_spark('3')
def test_correct_half_versus_regenie(spark):
    compare_to_regenie(spark,
                       0.5,
                       'test_bin_out_half_firth_',
                       compare_all_cols=False,
                       uncorrected=103,
                       corrected=97)


@pytest.mark.min_spark('3')
def test_correct_missing_versus_regenie(spark):
    compare_to_regenie(spark,
                       0.9999,
                       'test_bin_out_missing_firth_',
                       compare_all_cols=True,
                       uncorrected=0,
                       corrected=200,
                       missing=['35_35', '136_136', '77_77', '100_100', '204_204', '474_474'])
