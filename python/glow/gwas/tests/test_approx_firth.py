import glow.gwas.log_reg as lr
import glow.functions as fx
import glow.gwas.functions as gwas_fx
import pandas as pd
import numpy as np
import pytest


test_data_dir = 'test-data/regenie/'


def _read_fid_iid_df(file):
    df = pd.read_table(file, sep='\s+')
    df['FID_IID'] = df['FID'].astype(str) + '_' + df['IID'].astype(str)
    return df.drop(columns=['FID', 'IID']).set_index(['FID_IID']).sort_index(level=['FID_IID'])


def _read_offset_df(file, trait):
    return pd.melt(pd.read_table(file, sep='\s+'), id_vars=['FID_IID']) \
        .rename(columns={'FID_IID': 'contigName', 'variable': 'FID_IID', 'value': trait}) \
        .astype({'FID_IID': 'str', 'contigName': 'str'}) \
        .set_index(['FID_IID', 'contigName'])


@pytest.mark.min_spark('3')
def test_end_to_end(spark):
    genotype_df = spark.read.format('bgen').load(test_data_dir + 'example.bgen') \
        .withColumn('values', fx.genotype_states('genotypes')) \

    phenotype_df = _read_fid_iid_df(test_data_dir + 'phenotype_bin.txt')

    covariate_df = _read_fid_iid_df(test_data_dir + 'covariates.txt')

    offset_trait1_df = _read_offset_df(test_data_dir + 'fit_bin_out_1.loco', 'Y1')
    offset_trait2_df = _read_offset_df(test_data_dir + 'fit_bin_out_2.loco', 'Y2')
    offset_df = pd.merge(offset_trait1_df, offset_trait2_df, left_index=True, right_index=True)

    genotype_df.show()
    genotype_df.printSchema()
    print(phenotype_df)
    print(covariate_df)
    print(offset_df)

    glowgr_df = lr.logistic_regression(genotype_df,
                           phenotype_df,
                           covariate_df,
                           offset_df,
                           correction=lr.correction_approx_firth,
                           values_column='values').toPandas()
    print(glowgr_df)

    assert(False)
