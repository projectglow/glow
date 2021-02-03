# Copyright 2019 The Glow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import glow.functions as fx
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

test_data_dir = 'test-data/regenie/'


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
    df = pd.read_table(file, sep=r'\s+').astype({'ID': 'str'})
    df = df[df['GENPOS'] <= num_snps]
    df['phenotype'] = trait
    return df


def get_input_dfs(spark, binary, num_snps=100, missing=[], single_chr=True):
    if single_chr:
        genotype_file = 'example.bgen'
    else:
        genotype_file = 'example_3chr.bgen'
    if binary:
        phenotype_file = 'phenotype_bin.txt'
        offset_trait1_file = 'fit_bin_out_1.loco'
        offset_trait2_file = 'fit_bin_out_2.loco'
    else:
        phenotype_file = 'phenotype.txt'
        if single_chr:
            offset_trait1_file = 'fit_lin_out_1.loco'
            offset_trait2_file = 'fit_lin_out_2.loco'
        else:
            offset_trait1_file = 'fit_lin_out_3chr_1.loco'
            offset_trait2_file = 'fit_lin_out_3chr_2.loco'

    genotype_df = spark.read.format('bgen').load(test_data_dir + genotype_file) \
        .withColumn('values', fx.genotype_states('genotypes')) \
        .filter(f'start < {num_snps}')

    phenotype_df = _set_fid_iid_df(pd.read_table(test_data_dir + phenotype_file, sep=r'\s+'))
    phenotype_df.loc[missing, :] = np.nan

    covariate_df = _set_fid_iid_df(pd.read_table(test_data_dir + 'covariates.txt', sep=r'\s+'))

    offset_trait1_df = _read_offset_df(test_data_dir + offset_trait1_file, 'Y1')
    offset_trait2_df = _read_offset_df(test_data_dir + offset_trait2_file, 'Y2')
    offset_df = pd.merge(offset_trait1_df, offset_trait2_df, left_index=True, right_index=True)

    return (genotype_df, phenotype_df, covariate_df, offset_df)


def compare_to_regenie(output_prefix, glowgr_df, compare_all_cols=True, num_snps=100):
    regenie_files = [
        test_data_dir + output_prefix + 'Y1.regenie', test_data_dir + output_prefix + 'Y2.regenie'
    ]
    regenie_traits = ['Y1', 'Y2']
    regenie_df = pd.concat(
        [_read_regenie_df(f, t, num_snps) for f, t in zip(regenie_files, regenie_traits)],
        ignore_index=True)
    regenie_df['pvalue'] = np.power(10, -regenie_df['LOG10P'])

    glowgr_df['ID'] = glowgr_df['names'].apply(lambda x: str(x[-1]))
    glowgr_df = glowgr_df.rename(columns={'effect': 'BETA', 'stderror': 'SE'})

    regenie_df = regenie_df.set_index(['ID', 'phenotype'])
    glowgr_df = glowgr_df.set_index(['ID', 'phenotype']).reindex(regenie_df.index)

    if compare_all_cols:
        cols = ['BETA', 'SE', 'pvalue']
    else:
        cols = ['pvalue']
    assert np.isclose(glowgr_df[cols].to_numpy(), regenie_df[cols].to_numpy(), rtol=1e-2,
                      atol=1e-3).all()
