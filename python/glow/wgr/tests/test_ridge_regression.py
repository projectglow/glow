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
from glow.wgr.ridge_udfs import *
from glow.wgr.model_functions import _fill_na_and_standardize
import numpy as np
import pandas as pd
from pyspark.sql.functions import *
import math

data_root = 'test-data/wgr/ridge-regression'

X0 = pd.read_csv(f'{data_root}/X0.csv', dtype={'sample_id': 'str'}).set_index('sample_id')
X1 = pd.read_csv(f'{data_root}/X1.csv', dtype={'sample_id': 'str'}).set_index('sample_id')
X2 = pd.read_csv(f'{data_root}/X2.csv', dtype={'sample_id': 'str'}).set_index('sample_id')

labeldf = pd.read_csv(f'{data_root}/pts.csv', dtype={'sample_id': 'str'}).set_index('sample_id')
std_labeldf = _fill_na_and_standardize(labeldf)
label_with_missing = labeldf.copy()
label_with_missing.loc['1073199471', 'sim58'] = math.nan

n_cov = 2
cov_matrix = np.random.randn(*(labeldf.shape[0], n_cov))
covdf = pd.DataFrame(data=cov_matrix, columns=['cov1', 'cov2'], index=labeldf.index)
std_covdf = _fill_na_and_standardize(covdf)
covdf_empty = pd.DataFrame({})
covdf_with_missing = std_covdf.copy()
covdf_with_missing.loc['1073199471', 'cov1'] = math.nan

alphas = np.array([0.1, 1, 10])
alphaMap = {f'alpha_{i}': a for i, a in enumerate(alphas)}
columnIndexer = sorted(enumerate(alphaMap.keys()), key=lambda t: t[1])
coefOrder = [i for i, a in columnIndexer]

level1_yhat_loco_df = pd.read_csv(f'{data_root}/level1YHatLoco.csv',
                                  dtype={
                                      'sample_id': 'str',
                                      'contigName': 'str'
                                  }).set_index(['sample_id', 'contigName'])
level2_yhat_loco_df = pd.read_csv(f'{data_root}/level2YHatLoco.csv',
                                  dtype={
                                      'sample_id': 'str',
                                      'contigName': 'str'
                                  }).set_index(['sample_id', 'contigName'])


def __get_sample_blocks(indexdf):
    return {
        r.sample_block: r.sample_ids
        for r in indexdf.select('sample_block', 'sample_ids').collect()
    }


def __assert_dataframes_equal(df1, df2):
    assert df1.subtract(df2).count() == 0
    assert df2.subtract(df1).count() == 0


def test_map_normal_eqn(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet')
    testGroup = '0'
    testBlock = 'chr_1_block_0'
    ids = indexdf.filter(f'sample_block = {testGroup}').select('sample_ids').head().sample_ids
    headers = [
        r.header
        for r in blockdf.filter(f'header_block = "{testBlock}" AND sample_block = {testGroup}').
        orderBy('sort_key').select('header').collect()
    ]

    X_in = X0[headers].loc[ids, :]
    Y_in = labeldf.loc[ids, :]

    XtX_in = X_in.to_numpy().T @ X_in.to_numpy()
    XtY_in = X_in.to_numpy().T @ Y_in.to_numpy()

    sample_blocks = __get_sample_blocks(indexdf)
    map_key_pattern = ['header_block', 'sample_block']
    map_udf = pandas_udf(
        lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks,
                                        covdf_empty), normal_eqn_struct, PandasUDFType.GROUPED_MAP)

    outdf = blockdf \
        .groupBy(map_key_pattern) \
        .apply(map_udf) \
        .filter(f'header_block = "{testBlock}" AND sample_block = {testGroup}') \
        .orderBy('sort_key') \
        .select('xtx', 'xty') \
        .collect()

    XtX_in_lvl = np.array([r.xtx for r in outdf])
    XtY_in_lvl = np.array([r.xty for r in outdf])

    assert (np.allclose(XtX_in_lvl, XtX_in) and np.allclose(XtY_in_lvl, XtY_in))


def test_reduce_normal_eqn(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet')
    testGroup = '0'
    testBlock = 'chr_1_block_0'
    ids = indexdf.filter(f'sample_block = {testGroup}').select('sample_ids').head().sample_ids
    headers = [
        r.header
        for r in blockdf.filter(f'header_block = "{testBlock}" AND sample_block = {testGroup}').
        orderBy('sort_key').select('header').collect()
    ]

    X_out = X0[headers].drop(ids, axis='rows')
    Y_out = labeldf.drop(ids, axis='rows')

    XtX_out = X_out.to_numpy().T @ X_out.to_numpy()
    XtY_out = X_out.to_numpy().T @ Y_out.to_numpy()

    sample_blocks = __get_sample_blocks(indexdf)
    map_key_pattern = ['header_block', 'sample_block']
    reduce_key_pattern = ['header_block', 'header']
    map_udf = pandas_udf(
        lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks,
                                        covdf_empty), normal_eqn_struct, PandasUDFType.GROUPED_MAP)
    reduce_udf = pandas_udf(lambda key, pdf: reduce_normal_eqn(key, reduce_key_pattern, pdf),
                            normal_eqn_struct, PandasUDFType.GROUPED_MAP)

    mapdf = blockdf \
        .groupBy(map_key_pattern) \
        .apply(map_udf)

    outdf = mapdf.groupBy(reduce_key_pattern) \
        .apply(reduce_udf) \
        .filter(f'header_block = "{testBlock}" AND sample_block = {testGroup}') \
        .orderBy('sort_key') \
        .select('xtx', 'xty') \
        .collect()

    XtX_out_lvl = np.array([r.xtx for r in outdf])
    XtY_out_lvl = np.array([r.xty for r in outdf])

    assert (np.allclose(XtX_out_lvl, XtX_out) and np.allclose(XtY_out_lvl, XtY_out))


def test_solve_normal_eqn(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet')
    testGroup = '0'
    testBlock = 'chr_1_block_0'
    ids = indexdf.filter(f'sample_block = {testGroup}').select('sample_ids').head().sample_ids
    headers = [
        r.header
        for r in blockdf.filter(f'header_block = "{testBlock}" AND sample_block = {testGroup}').
        orderBy('sort_key').select('header').collect()
    ]

    X_out = X0[headers].drop(ids, axis='rows')
    Y_out = labeldf.drop(ids, axis='rows')

    XtX_out = X_out.to_numpy().T @ X_out.to_numpy()
    XtY_out = X_out.to_numpy().T @ Y_out.to_numpy()
    B = np.column_stack(
        [(np.linalg.inv(XtX_out + np.identity(XtX_out.shape[1]) * a) @ XtY_out) for a in alphas])

    sample_blocks = __get_sample_blocks(indexdf)
    map_key_pattern = ['header_block', 'sample_block']
    reduce_key_pattern = ['header_block', 'header']
    map_udf = pandas_udf(
        lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks,
                                        covdf_empty), normal_eqn_struct, PandasUDFType.GROUPED_MAP)
    reduce_udf = pandas_udf(lambda key, pdf: reduce_normal_eqn(key, reduce_key_pattern, pdf),
                            normal_eqn_struct, PandasUDFType.GROUPED_MAP)
    model_udf = pandas_udf(
        lambda key, pdf: solve_normal_eqn(key, reduce_key_pattern, pdf, labeldf, alphaMap,
                                          covdf_empty), model_struct, PandasUDFType.GROUPED_MAP)

    reducedf = blockdf \
        .groupBy(map_key_pattern) \
        .apply(map_udf) \
        .groupBy(reduce_key_pattern) \
        .apply(reduce_udf)

    columns = ['coefficients']
    rows = reducedf.groupBy(map_key_pattern) \
        .apply(model_udf) \
        .filter(f'header_block = "{testBlock}" AND sample_block = {testGroup}') \
        .select(*columns) \
        .collect()
    outdf = pd.DataFrame(rows, columns=columns)

    B_lvl = np.row_stack(outdf['coefficients'].to_numpy())

    assert np.allclose(B_lvl, B)


def test_apply_model(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet')
    testGroup = '0'
    testBlock = 'chr_1_block_0'
    ids = indexdf.filter(f'sample_block = {testGroup}').select('sample_ids').head().sample_ids
    headers = [
        r.header
        for r in blockdf.filter(f'header_block = "{testBlock}" AND sample_block = {testGroup}').
        orderBy('sort_key').select('header').collect()
    ]

    X_in = X0[headers].loc[ids, :]
    X_out = X0[headers].drop(ids, axis='rows')
    Y_out = labeldf.drop(ids, axis='rows')

    XtX_out = X_out.to_numpy().T @ X_out.to_numpy()
    XtY_out = X_out.to_numpy().T @ Y_out.to_numpy()
    B = np.column_stack(
        [(np.linalg.inv(XtX_out + np.identity(XtX_out.shape[1]) * a) @ XtY_out) for a in alphas])
    X1_in = X_in.to_numpy() @ B

    sample_blocks = __get_sample_blocks(indexdf)
    map_key_pattern = ['header_block', 'sample_block']
    reduce_key_pattern = ['header_block', 'header']
    transform_key_pattern = ['header_block', 'sample_block']
    map_udf = pandas_udf(
        lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks,
                                        covdf_empty), normal_eqn_struct, PandasUDFType.GROUPED_MAP)
    reduce_udf = pandas_udf(lambda key, pdf: reduce_normal_eqn(key, reduce_key_pattern, pdf),
                            normal_eqn_struct, PandasUDFType.GROUPED_MAP)
    model_udf = pandas_udf(
        lambda key, pdf: solve_normal_eqn(key, map_key_pattern, pdf, labeldf, alphaMap, covdf_empty),
        model_struct, PandasUDFType.GROUPED_MAP)
    transform_udf = pandas_udf(
        lambda key, pdf: apply_model(key, transform_key_pattern, pdf, labeldf, sample_blocks,
                                     alphaMap, covdf_empty), reduced_matrix_struct,
        PandasUDFType.GROUPED_MAP)

    modeldf = blockdf \
        .groupBy(map_key_pattern) \
        .apply(map_udf) \
        .groupBy(reduce_key_pattern) \
        .apply(reduce_udf) \
        .groupBy(map_key_pattern) \
        .apply(model_udf)

    columns = ['values']
    rows = blockdf.join(modeldf.drop('sort_key'), ['header_block', 'sample_block', 'header']) \
        .groupBy(transform_key_pattern) \
        .apply(transform_udf) \
        .filter(f'header LIKE "%{testBlock}%" AND sample_block = {testGroup}') \
        .select(*columns) \
        .collect()
    outdf = pd.DataFrame(rows, columns=columns)

    X1_in_lvl = np.column_stack(outdf['values'])

    assert np.allclose(X1_in_lvl, X1_in)


def test_ridge_reducer_fit(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet')
    testGroup = '0'
    testBlock = 'chr_1_block_0'
    ids = indexdf.filter(f'sample_block = {testGroup}').select('sample_ids').head().sample_ids
    headers = [
        r.header
        for r in blockdf.filter(f'header_block = "{testBlock}" AND sample_block = {testGroup}').
        orderBy('sort_key').select('header').collect()
    ]

    X_out = X0[headers].drop(ids, axis='rows')
    Y_out = std_labeldf.drop(ids, axis='rows')

    XtX_out = X_out.to_numpy().T @ X_out.to_numpy()
    XtY_out = X_out.to_numpy().T @ Y_out.to_numpy()
    B = np.column_stack(
        [(np.linalg.inv(XtX_out + np.identity(XtX_out.shape[1]) * a) @ XtY_out) for a in alphas])

    stack = RidgeReduction(blockdf,
                           labeldf,
                           __get_sample_blocks(indexdf),
                           add_intercept=False,
                           alphas=alphas)
    modeldf = stack.fit()

    columns = ['coefficients']
    rows = modeldf.filter(f'header_block = "{testBlock}" AND sample_block = {testGroup}') \
        .select(*columns).collect()
    outdf = pd.DataFrame(rows, columns=columns)

    B_stack = np.row_stack(outdf['coefficients'].to_numpy())

    print(B)
    print(B_stack)
    assert np.allclose(B_stack, B)


def test_ridge_reducer_transform(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet')
    sample_blocks = __get_sample_blocks(indexdf)
    testGroup = '0'
    testBlock = 'chr_1_block_0'
    ids = indexdf.filter(f'sample_block = {testGroup}').select('sample_ids').head().sample_ids
    headers = [
        r.header
        for r in blockdf.filter(f'header_block = "{testBlock}" AND sample_block = {testGroup}').
        orderBy('sort_key').select('header').collect()
    ]

    X_in = X0[headers].loc[ids, :]
    X_out = X0[headers].drop(ids, axis='rows')
    Y_out = std_labeldf.drop(ids, axis='rows')

    XtX_out = X_out.to_numpy().T @ X_out.to_numpy()
    XtY_out = X_out.to_numpy().T @ Y_out.to_numpy()
    B = np.column_stack(
        [(np.linalg.inv(XtX_out + np.identity(XtX_out.shape[1]) * a) @ XtY_out) for a in alphas])
    X1_in = X_in.to_numpy() @ B

    stack = RidgeReduction(blockdf, labeldf, sample_blocks, add_intercept=False, alphas=alphas)
    level1df = stack.fit_transform()

    columns = ['values']
    rows = level1df.filter(f'header LIKE "%{testBlock}%" AND sample_block = {testGroup}') \
        .select(*columns) \
        .collect()
    outdf = pd.DataFrame(rows, columns=columns)
    X1_in_stack = np.column_stack(outdf['values'])

    assert np.allclose(X1_in_stack, X1_in)


def test_ridge_reducer_transform_with_cov(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet')
    testGroup = '0'
    testBlock = 'chr_1_block_0'
    ids = indexdf.filter(f'sample_block = {testGroup}').select('sample_ids').head().sample_ids
    sample_blocks = __get_sample_blocks(indexdf)
    headers = [
        r.header
        for r in blockdf.filter(f'header_block = "{testBlock}" AND sample_block= {testGroup}').
        orderBy('sort_key').select('header').collect()
    ]

    C_in = std_covdf.loc[ids, :].values
    X_in = X0[headers].loc[ids, :].values
    X_in_cov = np.column_stack([C_in, X_in])
    C_out = std_covdf.drop(ids, axis='rows').values
    X_out = X0[headers].drop(ids, axis='rows').values
    X_out_cov = np.column_stack((C_out, X_out))
    Y_out = std_labeldf.drop(ids, axis='rows').values

    XtX_out_cov = X_out_cov.T @ X_out_cov
    XtY_out_cov = X_out_cov.T @ Y_out
    diags_cov = [
        np.concatenate([np.ones(n_cov), np.ones(XtX_out_cov.shape[1] - n_cov) * a]) for a in alphas
    ]
    B_cov = np.column_stack(
        [(np.linalg.inv(XtX_out_cov + np.diag(d)) @ XtY_out_cov) for d in diags_cov])
    X1_in_cov = X_in_cov @ B_cov

    stack = RidgeReduction(blockdf,
                           labeldf,
                           sample_blocks,
                           covdf,
                           add_intercept=False,
                           alphas=alphas)
    level1df_cov = stack.fit_transform()

    columns = ['alpha', 'label', 'values']
    rows_cov = level1df_cov.filter(f'header LIKE "%{testBlock}%" AND sample_block= {testGroup}') \
        .select(*columns) \
        .collect()
    outdf_cov = pd.DataFrame(rows_cov, columns=columns)
    X1_in_stack_cov = np.column_stack(outdf_cov['values'])

    assert np.allclose(X1_in_stack_cov, X1_in_cov)


def __calculate_y_hat(X_base, group2ids, testLabel, cov=covdf_empty):
    groups = sorted(group2ids.keys(), key=lambda v: v)
    cov_X = pd.concat([cov, X_base], axis=1, sort=True)
    headersToKeep = list(cov.columns) + [c for c in X_base.columns if testLabel in c]
    n_cov = len(cov.columns)

    r2s = []
    for group in groups:
        ids = group2ids[group]
        X_in = cov_X[headersToKeep].loc[ids, :].to_numpy()
        X_out = cov_X[headersToKeep].drop(ids, axis='rows')
        Y_in = std_labeldf[testLabel].loc[ids].to_numpy()
        Y_out = std_labeldf[testLabel].loc[X_out.index].to_numpy()
        XtX_out = X_out.to_numpy().T @ X_out.to_numpy()
        XtY_out = X_out.to_numpy().T @ Y_out
        diags = [
            np.concatenate([np.ones(n_cov), np.ones(XtX_out.shape[1] - n_cov) * a]) for a in alphas
        ]
        B = np.column_stack(
            [(np.linalg.inv(XtX_out + np.diag(d)) @ XtY_out) for d in diags])[:, coefOrder]
        XB = X_in @ B
        r2 = r_squared(XB, Y_in.reshape(-1, 1))
        r2s.append(r2)
    r2_mean = np.row_stack(r2s).mean(axis=0)

    bestAlpha, bestr2 = sorted(zip(alphaMap.keys(), r2_mean), key=lambda t: -t[1])[0]

    y_hat = pd.Series(index=std_labeldf.index)
    for group in groups:
        ids = group2ids[group]
        X_in = cov_X[headersToKeep].loc[ids, :].to_numpy()
        X_out = cov_X[headersToKeep].drop(ids, axis='rows')
        Y_out = std_labeldf[testLabel].loc[X_out.index].to_numpy()
        XtX_out = X_out.to_numpy().T @ X_out.to_numpy()
        XtY_out = X_out.to_numpy().T @ Y_out
        d = np.concatenate(
            [np.ones(n_cov),
             np.ones(XtX_out.shape[1] - n_cov) * alphaMap[bestAlpha]])
        b = np.linalg.inv(XtX_out + np.diag(d)) @ XtY_out
        group_y_hats = X_in @ b
        for s, y in zip(ids, group_y_hats):
            y_hat[s] = y

    return bestAlpha, bestr2, y_hat.to_numpy()


def test_one_level_regression(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet')
    testLabel = 'sim100'

    group2ids = __get_sample_blocks(indexdf)
    bestAlpha, bestr2, y_hat = __calculate_y_hat(X1, group2ids, testLabel)

    stack0 = RidgeReduction(blockdf, labeldf, group2ids, add_intercept=False, alphas=alphas)
    stack0.fit()
    stack0.transform()

    regressor = RidgeRegression.from_ridge_reduction(stack0, alphas)
    _, cvdf = regressor.fit()
    yhatdf = regressor.transform()

    r = cvdf.filter(f'label = "{testLabel}"').select('alpha', 'r2_mean').head()
    bestAlpha_lvl, bestr2_lvl = (r.alpha, r.r2_mean)
    y_hat_lvl = np.array(yhatdf[testLabel])

    assert (bestAlpha_lvl == bestAlpha and np.isclose(bestr2_lvl, bestr2) and
            np.allclose(y_hat_lvl, np.array(y_hat)))


def test_two_level_regression(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level2df = spark.read.parquet(f'{data_root}/level2BlockedGT.snappy.parquet')
    testLabel = 'sim100'

    group2ids = __get_sample_blocks(indexdf)
    bestAlpha, bestr2, y_hat = __calculate_y_hat(X2, group2ids, testLabel)

    regressor = RidgeRegression(level2df, labeldf, group2ids, add_intercept=False, alphas=alphas)
    _, cvdf = regressor.fit()
    yhatdf = regressor.transform()

    r = cvdf.filter(f'label = "{testLabel}"').select('alpha', 'r2_mean').head()
    bestAlpha_lvl, bestr2_lvl = (r.alpha, r.r2_mean)
    y_hat_lvl = np.array(yhatdf[testLabel])

    assert (bestAlpha_lvl == bestAlpha and np.isclose(bestr2_lvl, bestr2) and
            np.allclose(y_hat_lvl, np.array(y_hat)))


def test_two_level_regression_with_cov(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level2df = spark.read.parquet(f'{data_root}/level2BlockedGT.snappy.parquet')
    testLabel = 'sim100'

    group2ids = __get_sample_blocks(indexdf)
    bestAlpha, bestr2, y_hat = __calculate_y_hat(X2, group2ids, testLabel, std_covdf)

    regressor = RidgeRegression(level2df,
                                labeldf,
                                group2ids,
                                std_covdf,
                                add_intercept=False,
                                alphas=alphas)
    _, cvdf = regressor.fit()
    yhatdf = regressor.transform()

    r = cvdf.filter(f'label = "{testLabel}"').select('alpha', 'r2_mean').head()
    bestAlpha_lvl, bestr2_lvl = (r.alpha, r.r2_mean)
    y_hat_lvl = np.array(yhatdf[testLabel])

    assert (bestAlpha_lvl == bestAlpha and np.isclose(bestr2_lvl, bestr2) and
            np.allclose(y_hat_lvl, np.array(y_hat)))


def test_tie_break(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level1df = spark.read.parquet(f'{data_root}/level1BlockedGT.snappy.parquet').limit(5)
    group2ids = __get_sample_blocks(indexdf)

    regressor = RidgeRegression(level1df,
                                std_labeldf,
                                group2ids,
                                add_intercept=False,
                                alphas=np.array([0.1, 0.2, 0.1, 0.2]))
    _, cvdf = regressor.fit()

    assert cvdf.count() == len(labeldf.columns)


def test_reducer_fit_transform(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet').limit(5)
    group2ids = __get_sample_blocks(indexdf)

    stack0 = RidgeReduction(blockdf, std_labeldf, group2ids, add_intercept=False, alphas=alphas)
    stack0.fit()
    level1df = stack0.transform()
    fit_transform_df = stack0.fit_transform()

    __assert_dataframes_equal(fit_transform_df, level1df)


def test_regression_fit_transform(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level1df = spark.read.parquet(f'{data_root}/level1BlockedGT.snappy.parquet').limit(5)
    group2ids = __get_sample_blocks(indexdf)

    regressor = RidgeRegression(level1df,
                                std_labeldf,
                                group2ids,
                                add_intercept=False,
                                alphas=alphas)
    yhatdf = regressor.fit_transform()
    fit_transform_df = regressor.fit_transform()

    assert fit_transform_df.equals(yhatdf)


def test_reducer_generate_alphas(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet').limit(5)
    group2ids = __get_sample_blocks(indexdf)

    stack_without_alphas = RidgeReduction(blockdf, std_labeldf, group2ids, add_intercept=False)
    stack_with_alphas = RidgeReduction(blockdf,
                                       std_labeldf,
                                       group2ids,
                                       add_intercept=False,
                                       alphas=np.array(
                                           sorted(list(generate_alphas(blockdf).values()))))

    model0_without_alphas = stack_without_alphas.fit()
    model0df = stack_with_alphas.fit()
    __assert_dataframes_equal(model0_without_alphas, model0df)

    level1_without_alphas = stack_without_alphas.transform()
    level1df = stack_with_alphas.transform()
    __assert_dataframes_equal(level1_without_alphas, level1df)


def test_regression_generate_alphas(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level1df = spark.read.parquet(f'{data_root}/level1BlockedGT.snappy.parquet').limit(5)
    group2ids = __get_sample_blocks(indexdf)

    regressor_without_alphas = RidgeRegression(level1df,
                                               std_labeldf,
                                               group2ids,
                                               add_intercept=False)
    regressor_with_alphas = RidgeRegression(level1df,
                                            std_labeldf,
                                            group2ids,
                                            add_intercept=False,
                                            alphas=np.array(
                                                sorted(list(generate_alphas(level1df).values()))))

    model1_without_alphas, cv_without_alphas = regressor_without_alphas.fit()
    model1df, cvdf = regressor_with_alphas.fit()
    __assert_dataframes_equal(model1_without_alphas, model1df)
    __assert_dataframes_equal(cv_without_alphas, cvdf)

    yhat_without_alphas = regressor_without_alphas.transform()
    yhatdf = regressor_with_alphas.transform()
    assert yhat_without_alphas.equals(yhatdf)


def test_one_level_regression_transform_loco_provide_contigs(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level1df = spark.read.parquet(f'{data_root}/level1BlockedGT.snappy.parquet')

    group2ids = __get_sample_blocks(indexdf)
    regressor = RidgeRegression(level1df, labeldf, group2ids, add_intercept=False, alphas=alphas)
    y_hat = regressor.fit_transform_loco(chromosomes=['1', '2', '3'])

    pd.testing.assert_frame_equal(y_hat, level1_yhat_loco_df)


def test_one_level_regression_transform_loco_infer_contigs(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level1df = spark.read.parquet(f'{data_root}/level1BlockedGT.snappy.parquet')

    group2ids = __get_sample_blocks(indexdf)
    regressor = RidgeRegression(level1df, labeldf, group2ids, add_intercept=False, alphas=alphas)
    y_hat = regressor.fit_transform_loco()

    pd.testing.assert_frame_equal(y_hat, level1_yhat_loco_df)


def test_two_level_regression_transform_loco_provide_contigs(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level2df = spark.read.parquet(f'{data_root}/level2BlockedGT.snappy.parquet')

    group2ids = __get_sample_blocks(indexdf)
    regressor = RidgeRegression(level2df, labeldf, group2ids, add_intercept=False, alphas=alphas)
    y_hat = regressor.fit_transform_loco(chromosomes=['1', '2', '3'])

    pd.testing.assert_frame_equal(y_hat, level2_yhat_loco_df)


def test_two_level_regression_transform_loco_infer_contigs(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level2df = spark.read.parquet(f'{data_root}/level2BlockedGT.snappy.parquet')

    group2ids = __get_sample_blocks(indexdf)
    regressor = RidgeRegression(level2df, labeldf, group2ids, add_intercept=False, alphas=alphas)
    y_hat = regressor.fit_transform_loco()

    pd.testing.assert_frame_equal(y_hat, level2_yhat_loco_df)


def test_model_cv_df(spark):
    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    level1df = spark.read.parquet(f'{data_root}/level1BlockedGT.snappy.parquet').limit(5)
    group2ids = __get_sample_blocks(indexdf)

    regressor = RidgeRegression(level1df,
                                std_labeldf,
                                group2ids,
                                add_intercept=False,
                                alphas=alphas)

    regressor.model_df = spark.createDataFrame([('Alice', 1)])
    model_df = spark.createDataFrame([('Alice', 1)])
    regressor.model_df = model_df

    cv_df = spark.createDataFrame([('Bob', 2)])
    regressor.cv_df = cv_df

    assert str(regressor.model_df.storageLevel) == 'Serialized 1x Replicated'
    regressor._cache_model_cv_df()
    assert str(regressor.model_df.storageLevel) == 'Disk Memory Deserialized 1x Replicated'
    regressor._unpersist_model_cv_df()
    assert str(regressor.model_df.storageLevel) == 'Serialized 1x Replicated'
