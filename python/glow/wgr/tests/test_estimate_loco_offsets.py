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

from glow.wgr.ridge_reduction import *
from glow.wgr.ridge_regression import *
from glow.wgr.logistic_ridge_regression import *
from glow.wgr.wgr_functions import estimate_loco_offsets
import numpy as np
import pandas as pd

ridge_data_root = 'test-data/wgr/ridge-regression'
logistic_ridge_data_root = 'test-data/wgr/logistic-regression'

alphas = np.array([0.1, 1, 10])


def __get_sample_blocks(indexdf):
    return {
        r.sample_block: r.sample_ids
        for r in indexdf.select('sample_block', 'sample_ids').collect()
    }


def test_estimate_loco_offsets_ridge(spark):
    labeldf = pd.read_csv(f'{ridge_data_root}/pts.csv', dtype={
        'sample_id': 'str'
    }).set_index('sample_id')
    indexdf = spark.read.parquet(f'{ridge_data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{ridge_data_root}/blockedGT.snappy.parquet')

    group2ids = __get_sample_blocks(indexdf)

    y_hat_df = estimate_loco_offsets(blockdf,
                                     labeldf,
                                     group2ids,
                                     add_intercept=False,
                                     reduction_alphas=alphas,
                                     regression_alphas=alphas)

    stack0 = RidgeReduction(blockdf, labeldf, group2ids, add_intercept=False, alphas=alphas)
    stack0.fit_transform()
    regressor = RidgeRegression.from_ridge_reduction(stack0, alphas)
    yhatdf = regressor.fit_transform_loco()

    assert (np.allclose(y_hat_df, yhatdf))


def test_estimate_loco_offsets_logistic_ridge_no_intercept_no_cov(spark):
    labeldf = pd.read_csv(f'{logistic_ridge_data_root}/binary_phenotypes.csv').set_index('sample_id')
    labeldf.index = labeldf.index.astype(str, copy=False)
    indexdf = spark.read.parquet(f'{ridge_data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{ridge_data_root}/blockedGT.snappy.parquet')

    group2ids = __get_sample_blocks(indexdf)

    y_hat_df = estimate_loco_offsets(blockdf,
                                     labeldf,
                                     group2ids,
                                     add_intercept=False,
                                     reduction_alphas=alphas,
                                     regression_alphas=alphas)

    stack0 = RidgeReduction(blockdf, labeldf, group2ids, add_intercept=False, alphas=alphas)
    stack0.fit_transform()
    regressor = LogisticRidgeRegression.from_ridge_reduction(stack0, alphas)
    yhatdf = regressor.fit_transform_loco()

    assert (np.allclose(y_hat_df, yhatdf))


def test_estimate_loco_offsets_logistic_ridge_with_intercept_no_cov(spark):
    labeldf = pd.read_csv(f'{logistic_ridge_data_root}/binary_phenotypes.csv').set_index('sample_id')
    labeldf.index = labeldf.index.astype(str, copy=False)
    indexdf = spark.read.parquet(f'{ridge_data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{ridge_data_root}/blockedGT.snappy.parquet')

    group2ids = __get_sample_blocks(indexdf)

    y_hat_df = estimate_loco_offsets(blockdf,
                                     labeldf,
                                     group2ids,
                                     reduction_alphas=alphas,
                                     regression_alphas=alphas)

    stack0 = RidgeReduction(blockdf, labeldf, group2ids, alphas=alphas)
    stack0.fit_transform()
    regressor = LogisticRidgeRegression.from_ridge_reduction(stack0, alphas)
    yhatdf = regressor.fit_transform_loco()

    assert (np.allclose(y_hat_df, yhatdf))


def test_estimate_loco_offsets_logistic_ridge_no_intercept_with_cov(spark):
    covdf = pd.read_csv(f'{logistic_ridge_data_root}/cov.csv').set_index('sample_id')
    covdf.index = covdf.index.astype(str, copy=False)
    labeldf = pd.read_csv(f'{logistic_ridge_data_root}/binary_phenotypes.csv').set_index('sample_id')
    labeldf.index = labeldf.index.astype(str, copy=False)
    indexdf = spark.read.parquet(f'{ridge_data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{ridge_data_root}/blockedGT.snappy.parquet')

    group2ids = __get_sample_blocks(indexdf)

    y_hat_df = estimate_loco_offsets(blockdf,
                                     labeldf,
                                     group2ids,
                                     covdf,
                                     add_intercept=False,
                                     reduction_alphas=alphas,
                                     regression_alphas=alphas)

    stack0 = RidgeReduction(blockdf, labeldf, group2ids, covdf, add_intercept=False, alphas=alphas)
    stack0.fit_transform()
    regressor = LogisticRidgeRegression.from_ridge_reduction(stack0, alphas)
    yhatdf = regressor.fit_transform_loco()
    print(y_hat_df)

    assert (np.allclose(y_hat_df, yhatdf))


def test_estimate_loco_offsets_logistic_ridge_with_intercept_with_cov(spark):
    covdf = pd.read_csv(f'{logistic_ridge_data_root}/cov.csv').set_index('sample_id')
    covdf.index = covdf.index.astype(str, copy=False)
    labeldf = pd.read_csv(f'{logistic_ridge_data_root}/binary_phenotypes.csv').set_index('sample_id')
    labeldf.index = labeldf.index.astype(str, copy=False)
    indexdf = spark.read.parquet(f'{ridge_data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{ridge_data_root}/blockedGT.snappy.parquet')

    group2ids = __get_sample_blocks(indexdf)

    y_hat_df = estimate_loco_offsets(blockdf,
                                     labeldf,
                                     group2ids,
                                     covdf,
                                     reduction_alphas=alphas,
                                     regression_alphas=alphas)

    stack0 = RidgeReduction(blockdf, labeldf, group2ids, covdf, alphas=alphas)
    stack0.fit_transform()
    regressor = LogisticRidgeRegression.from_ridge_reduction(stack0, alphas)
    yhatdf = regressor.fit_transform_loco()

    assert (np.allclose(y_hat_df, yhatdf))
