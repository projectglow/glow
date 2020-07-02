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

from glow.wgr.linear_model.functions import *
import math
import numpy as np
import pandas as pd
from pyspark.sql import Row
import pytest


def test_sort_by_numeric():
    nums = np.random.rand(1000)
    df = pd.DataFrame({'nums': nums})
    df_copy = df.copy(deep=True)
    sort_in_place(df, ['nums'])
    df_copy.sort_values('nums', inplace=True)
    assert (df['nums'].array == df_copy['nums'].array).all()


def test_sort_by_string():
    nums = np.random.rand(1000)
    strings = [str(n) for n in nums]
    df = pd.DataFrame({'nums': nums, 'strings': strings})
    df_copy = df.copy(deep=True)
    sort_in_place(df, ['strings'])
    df_copy.sort_values('strings', inplace=True)
    assert (df['nums'].array == df_copy['nums'].array).all()
    assert (df['strings'].array == df_copy['strings'].array).all()


def test_sort_by_multiple_columns():
    nums = np.random.rand(1000) * 10
    df = pd.DataFrame({'nums': nums})
    df['bin'] = df['nums'] // 1
    df_copy = df.copy(deep=True)
    sort_in_place(df, ['bin', 'nums'])
    df_copy.sort_values(['bin', 'nums'], inplace=True)
    assert (df['nums'].array == df_copy['nums'].array).all()


def test_assemble_block():
    df = pd.DataFrame({'mu': [0.2], 'sig': [0.1], 'values': [[0.1, 0.3]]})
    block = assemble_block(n_rows=1, n_cols=2, pdf=df, cov_matrix=np.array([[]]))
    assert np.allclose(block, np.array([[-1.], [1.]]))


def test_assemble_block_zero_sig():
    df = pd.DataFrame({'mu': [0.2, 0], 'sig': [0.1, 0], 'values': [[0.1, 0.3], [0, 0]]})
    with pytest.raises(ValueError):
        assemble_block(n_rows=2, n_cols=2, pdf=df, cov_matrix=np.array([[]]))


def test_generate_alphas(spark):
    df = spark.createDataFrame(
        [Row(header='header_one'),
         Row(header='header_one'),
         Row(header='header_two')])
    expected_alphas = {
        'alpha_0': np.float(2 / 0.99),
        'alpha_1': np.float(2 / 0.75),
        'alpha_2': np.float(2 / 0.5),
        'alpha_3': np.float(2 / 0.25),
        'alpha_4': np.float(2 / 0.01)
    }
    assert generate_alphas(df) == expected_alphas


def test_assert_labels_all_present(spark):
    labeldf = pd.DataFrame({'Trait_1': [-1, 1], 'Trait_2': [1, math.nan]})
    covdf = pd.DataFrame({'Covariate_1': [1, -1], 'Covariate_2': [-1, 1]})
    with pytest.raises(ValueError):
        validate_inputs(labeldf, covdf)


def test_assert_covars_all_present(spark):
    labeldf = pd.DataFrame({'Trait_1': [-1, 1], 'Trait_2': [1, -1]})
    covdf = pd.DataFrame({'Covariate_1': [1, -1], 'Covariate_2': [-1, math.nan]})
    with pytest.raises(ValueError):
        validate_inputs(labeldf, covdf)


def test_check_labels_zero_mean(spark):
    labeldf = pd.DataFrame({'Trait_1': [-1, 1], 'Trait_2': [1, 3]})
    covdf = pd.DataFrame({'Covariate_1': [1, -1], 'Covariate_2': [-1, 1]})
    with pytest.warns(UserWarning):
        validate_inputs(labeldf, covdf)


def test_check_labels_unit_stddev(spark):
    labeldf = pd.DataFrame({'Trait_1': [-1, 1], 'Trait_2': [2, -2]})
    covdf = pd.DataFrame({'Covariate_1': [1, -1], 'Covariate_2': [-1, 1]})
    with pytest.warns(UserWarning):
        validate_inputs(labeldf, covdf)


def test_check_covars_zero_mean(spark):
    labeldf = pd.DataFrame({'Trait_1': [-1, 1], 'Trait_2': [1, -1]})
    covdf = pd.DataFrame({'Covariate_1': [1, -1], 'Covariate_2': [3, 1]})
    with pytest.warns(UserWarning):
        validate_inputs(labeldf, covdf)


def test_check_covars_unit_stddev(spark):
    labeldf = pd.DataFrame({'Trait_1': [-1, 1], 'Trait_2': [1, -1]})
    covdf = pd.DataFrame({'Covariate_1': [1, -1], 'Covariate_2': [-2, 2]})
    with pytest.warns(UserWarning):
        validate_inputs(labeldf, covdf)


def test_new_headers_one_level(spark):
    (new_header_block, sort_keys, headers) = new_headers(
        'chr_decoy_1_block_10', ['alpha_1', 'alpha_2'], [('alpha_1', 'sim1'), ('alpha_1', 'sim2'),
                                                         ('alpha_2', 'sim1'), ('alpha_2', 'sim2')])
    assert new_header_block == 'chr_decoy_1'
    assert sort_keys == [10 * 2 + 1, 10 * 2 + 1, 10 * 2 + 2, 10 * 2 + 2]
    assert headers == [
        'chr_decoy_1_block_10_alpha_1_label_sim1', 'chr_decoy_1_block_10_alpha_1_label_sim2',
        'chr_decoy_1_block_10_alpha_2_label_sim1', 'chr_decoy_1_block_10_alpha_2_label_sim2'
    ]


def test_new_headers_two_level(spark):
    (new_header_block, sort_keys, headers) = new_headers('chr_decoy_1', ['alpha_1', 'alpha_2'],
                                                         [('alpha_1', 'sim1'), ('alpha_1', 'sim2'),
                                                          ('alpha_2', 'sim1'), ('alpha_2', 'sim2')])
    assert new_header_block == 'all'
    contig_hash = abs(hash('decoy_1')) % (10**8)
    assert sort_keys == [
        contig_hash * 2 + 1, contig_hash * 2 + 1, contig_hash * 2 + 2, contig_hash * 2 + 2
    ]
    assert headers == [
        'chr_decoy_1_alpha_1_label_sim1', 'chr_decoy_1_alpha_1_label_sim2',
        'chr_decoy_1_alpha_2_label_sim1', 'chr_decoy_1_alpha_2_label_sim2'
    ]


def test_new_headers_three_levels(spark):
    (new_header_block, sort_keys, headers) = new_headers('all', ['alpha_1', 'alpha_2'],
                                                         [('alpha_1', 'sim1'), ('alpha_1', 'sim2'),
                                                          ('alpha_2', 'sim1'), ('alpha_2', 'sim2')])
    assert new_header_block == 'all'
    assert sort_keys == [0 * 2 + 1, 0 * 2 + 1, 0 * 2 + 2, 0 * 2 + 2]
    assert headers == [
        'alpha_1_label_sim1', 'alpha_1_label_sim2', 'alpha_2_label_sim1', 'alpha_2_label_sim2'
    ]


def test_infer_chromosomes(spark):
    df = spark.createDataFrame([
        Row(header='chr_3_block_8_alpha_0_label_sim100'),
        Row(header='chr_3_alpha_0_label_sim100'),
        Row(header='chr_X_alpha_0_label_sim100'),
        Row(header='chr_decoy_1_block_8_alpha_0_label_sim100'),
        Row(header='chr_decoy_2_alpha_0_label_sim100')
    ])
    assert sorted(infer_chromosomes(df)) == ['3', 'X', 'decoy_1', 'decoy_2']
