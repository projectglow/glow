import glow.gwas.log_reg as lr
import glow.gwas.approx_firth as af
import glow.functions as fx
import pandas as pd
from pandas.testing import assert_frame_equal
from pyspark.ml.linalg import DenseMatrix
from pyspark.sql import Row
import numpy as np
import pytest


default_phenotypes = [1, 0, 0, 1, 1, 1, 1, 0]
default_genotypes = [0, 0, 1, 2, 2, 1, 1, 1]
default_covariates = [[1, 1, 1, 1, 1, 1, 1, 1], [2, 3, 2, 3, 2, 3, 2, 3], [-1, -2, -3, -1, -2, -3, -1, -2]]
default_offset = [0.1, 0.2, 0.3, 0.4, 0.4, 0.3, 0.2, 0.1]


def get_golden_firth_fit(
        spark,
        phenotypes=default_phenotypes,
        genotypes=default_genotypes,
        covariates=default_covariates,
        offset=default_offset):

    covariate_matrix = DenseMatrix(
        numRows=len(covariates[0]),
        numCols=len(covariates),
        values=list(np.ravel(covariates))
    )
    df = spark.createDataFrame([Row(
        genotypes=genotypes,
        phenotypes=phenotypes,
        covariates=covariate_matrix,
        offset=offset
    )])
    return df.select(fx.expand_struct(
        fx.logistic_regression_gwas('genotypes', 'phenotypes', 'covariates', 'Firth', 'offset')
    ))


def _compare_full_firth_beta(
        spark,
        phenotypes=default_phenotypes,
        genotypes=default_genotypes,
        covariates=default_covariates,
        offset=default_offset):

    golden_firth_fit = get_golden_firth_fit(spark, phenotypes, genotypes, covariates, offset)

    test_firth_fit = af._fit_firth(
        beta_init=np.zeros(len(covariates) + 1),
        X=np.column_stack([np.array(covariates).T, genotypes]),
        y=np.array(phenotypes),
        offset=np.array(offset),
    )
    golden_firth_beta = golden_firth_fit.head().beta
    test_firth_beta = test_firth_fit.beta[-1]
    assert np.allclose(golden_firth_beta, test_firth_beta)


def test_full_firth(spark):
    _compare_full_firth_beta(spark)


def test_full_firth_no_offset(spark):
    _compare_full_firth_beta(spark, offset=[0]*len(default_offset))


def test_full_firth_no_intercept(spark):
    _compare_full_firth_beta(spark, covariates=default_covariates[1:])


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


@pytest.mark.min_spark('3')
def test_regenie(spark):
    test_data_dir = 'test-data/regenie/'

    num_snps = 100 # Spot check

    genotype_df = spark.read.format('bgen').load(test_data_dir + 'example.bgen') \
        .withColumn('values', fx.genotype_states('genotypes')) \
        .filter(f'start < {num_snps}')

    phenotype_df = _set_fid_iid_df(pd.read_table(test_data_dir + 'phenotype_bin.txt', sep=r'\s+'))

    covariate_df = _set_fid_iid_df(pd.read_table(test_data_dir + 'covariates.txt', sep=r'\s+'))

    offset_trait1_df = _read_offset_df(test_data_dir + 'fit_bin_out_1.loco', 'Y1')
    offset_trait2_df = _read_offset_df(test_data_dir + 'fit_bin_out_2.loco', 'Y2')
    offset_df = pd.merge(offset_trait1_df, offset_trait2_df, left_index=True, right_index=True)

    glowgr_df = lr.logistic_regression(genotype_df,
                           phenotype_df,
                           covariate_df,
                           offset_df,
                           correction=lr.correction_approx_firth,
                           pvalue_threshold=0.9999, # correct all SNPs
                           values_column='values').toPandas()

    regenie_files = [test_data_dir + 'test_bin_out_firth_Y1.regenie', test_data_dir + 'test_bin_out_firth_Y2.regenie']
    regenie_traits = ['Y1', 'Y2']
    regenie_df = pd.concat(
        [_read_regenie_df(f, t, num_snps) for f, t in zip(regenie_files, regenie_traits)],
        ignore_index=True
    )

    glowgr_df['ID'] = glowgr_df['names'].apply(lambda x: int(x[-1]))
    glowgr_df = glowgr_df.rename(columns={'effect': 'BETA', 'stderr': 'SE'}).astype({'ID': 'int64'})
    regenie_df['pvalue'] = np.power(10, -regenie_df['LOG10P'])
    assert_frame_equal(
        glowgr_df[['ID', 'BETA', 'SE', 'pvalue', 'phenotype']],
        regenie_df[['ID', 'BETA', 'SE', 'pvalue', 'phenotype']],
        check_dtype=False,
        check_less_precise=True
    )
