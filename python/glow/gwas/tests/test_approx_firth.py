import glow.gwas.log_reg as lr
import glow.gwas.approx_firth as af
import glow.functions as fx
import glow.gwas.functions as gwas_fx
import pandas as pd
from pyspark.ml.linalg import DenseMatrix
from pyspark.sql import Row
from pyspark.sql.functions import col, lit
import numpy as np
import pytest


default_phenotypes = [1, 0, 0, 1, 1, 1, 1, 0]
default_genotypes = [0, 0, 1, 2, 2, 1, 1, 1]
default_covariates = [[1, 1, 1, 1, 1, 1, 1, 1], [2, 3, 2, 3, 2, 3, 2, 3], [-1, -2, -3, -1, -2, -3, -1, -2]]
default_offset = [0.1, 0.2, 0.3, 0.4, 0.4, 0.3, 0.2, 0.1]


def _read_fid_iid_df(file):
    df = pd.read_table(file, sep='\s+')
    df['FID_IID'] = df['FID'].astype(str) + '_' + df['IID'].astype(str)
    return df.drop(columns=['FID', 'IID']).set_index(['FID_IID']).sort_index(level=['FID_IID'])


def _read_offset_df(file, trait):
    return pd.melt(pd.read_table(file, sep='\s+'), id_vars=['FID_IID']) \
        .rename(columns={'FID_IID': 'contigName', 'variable': 'FID_IID', 'value': trait}) \
        .astype({'FID_IID': 'str', 'contigName': 'str'}) \
        .set_index(['FID_IID', 'contigName'])


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


def compare_full_firth_beta(
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
        y_mask=[1.] * len(phenotypes),
    )
    golden_firth_beta = golden_firth_fit.head().beta
    test_firth_beta = test_firth_fit.beta[-1]
    print(golden_firth_fit.head())
    print(golden_firth_beta)
    assert False
    assert np.allclose(golden_firth_beta, test_firth_beta)


def test_full_firth(spark):
    compare_full_firth_beta(spark)

#
# def test_full_firth_no_offset(spark):
#     compare_full_firth_beta(spark, offset=[0]*len(default_offset))
#
#
# def test_full_firth_no_intercept(spark):
#     compare_full_firth_beta(spark, covariates=default_covariates[1:])


def test_snp_fit(spark, rg):
    n_sample = 1000
    n_pheno = 2
    n_geno = 2
    n_cov = 20
    phenotypes = rg.integers(low=0, high=2, size=(n_sample, n_pheno)).astype(np.float64)
    genotypes = rg.random((n_sample, n_geno))
    covariates = rg.random((n_sample, n_cov))
    offset = rg.random((n_sample, n_pheno))

    golden_snp_fit = get_golden_firth_fit(
        spark,
        phenotypes=phenotypes.tolist(),
        genotypes=genotypes.tolist(),
        covariates=covariates.tolist(),
        offset=offset.tolist()
    )
    approx_fit_state = af.create_approx_firth_state(
        Y=phenotypes,
        offset_df=pd.DataFrame(offset),
        C=covariates,
        Y_mask=np.ones(phenotypes.shape),
        fit_intercept=False
    )
    lr._logistic_regression_inner()
    assert False


@pytest.mark.min_spark('3')
def test_end_to_end(spark):
    test_data_dir = 'test-data/regenie/'

    genotype_df = spark.read.format('bgen').load(test_data_dir + 'example.bgen') \
        .withColumn('values', fx.genotype_states('genotypes'))

    phenotype_df = _read_fid_iid_df(test_data_dir + 'phenotype_bin.txt')

    covariate_df = _read_fid_iid_df(test_data_dir + 'covariates.txt')

    offset_trait1_df = _read_offset_df(test_data_dir + 'fit_bin_out_1.loco', 'Y1')
    offset_trait2_df = _read_offset_df(test_data_dir + 'fit_bin_out_2.loco', 'Y2')
    offset_df = pd.merge(offset_trait1_df, offset_trait2_df, left_index=True, right_index=True)

    # glowgr_df = lr.logistic_regression(genotype_df,
    #                        phenotype_df,
    #                        covariate_df,
    #                        offset_df,
    #                        correction=lr.correction_approx_firth,
    #                        values_column='values').toPandas()
    # print(glowgr_df)

    assert(False)
