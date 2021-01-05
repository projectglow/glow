import glow.gwas.log_reg as lr
import glow.functions as fx
import glow.gwas.functions as gwas_fx
import pandas as pd
from pyspark.ml.linalg import DenseMatrix
from pyspark.sql import Row
from pyspark.sql.functions import col, lit
import numpy as np
import pytest


def _read_fid_iid_df(file):
    df = pd.read_table(file, sep='\s+')
    df['FID_IID'] = df['FID'].astype(str) + '_' + df['IID'].astype(str)
    return df.drop(columns=['FID', 'IID']).set_index(['FID_IID']).sort_index(level=['FID_IID'])


def _read_offset_df(file, trait):
    return pd.melt(pd.read_table(file, sep='\s+'), id_vars=['FID_IID']) \
        .rename(columns={'FID_IID': 'contigName', 'variable': 'FID_IID', 'value': trait}) \
        .astype({'FID_IID': 'str', 'contigName': 'str'}) \
        .set_index(['FID_IID', 'contigName'])


def test_versus_firth(spark):
    phenotypes = [1, 0, 0, 1, 1]
    genotypes = [0, 0, 1, 2, 2]
    offset = [0.1, 0.2, 0.3, 0.2, 0.1]
    covariates = [1, 1, 1, 1, 1]
    covariate_matrix = DenseMatrix(numRows=5, numCols=1, values=covariates)
    df = spark.createDataFrame([Row(genotypes=genotypes, phenotypes=phenotypes, covariates=covariate_matrix, offset=offset)])
    full_firth_fit = df.select(fx.expand_struct(
        fx.logistic_regression_gwas('genotypes', 'phenotypes', 'covariates', 'Firth', 'offset')
    ))
    approx_firth_fit = lr.logistic_regression(
        spark.createDataFrame([Row(values=genotypes)]),
        pd.DataFrame(phenotypes),
        pd.DataFrame(covariates),
        pd.DataFrame(offset),
        correction=lr.correction_approx_firth,
        fit_intercept=False,
        pvalue_threshold=1.0,
        values_column='values'
    )
    full_firth_fit.show()
    approx_firth_fit.show()
    assert(False)



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
