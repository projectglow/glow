from glow.levels.linear_model import RidgeReducer, RidgeRegression
from glow.levels.linear_model.ridge_model import *
import pandas as pd

data_root = 'test-data/levels/ridge-regression'
X0 = pd.read_csv(f'{data_root}/X0.csv').set_index('sample_id')
X1 = pd.read_csv(f'{data_root}/X1.csv').set_index('sample_id')
X2 = pd.read_csv(f'{data_root}/X2.csv').set_index('sample_id')
labeldf = pd.read_csv(f'{data_root}/pts.csv').set_index('sample_id')
alphas = np.array([0.1, 1, 10])
alphaMap = {f'alpha_{i}': a for i, a in enumerate(alphas)}


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

    XtX_in = X_in.values.T @ X_in.values
    XtY_in = X_in.values.T @ Y_in.values

    sample_index = {
        r.sample_block: r.sample_ids
        for r in indexdf.select('sample_block', 'sample_ids').collect()
    }
    map_key_pattern = ['header_block', 'sample_block']
    map_udf = pandas_udf(
        lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, labeldf, sample_index),
        normal_eqn_struct, PandasUDFType.GROUPED_MAP)

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

    XtX_out = X_out.values.T @ X_out.values
    XtY_out = X_out.values.T @ Y_out.values

    sample_index = {
        r.sample_block: r.sample_ids
        for r in indexdf.select('sample_block', 'sample_ids').collect()
    }
    map_key_pattern = ['header_block', 'sample_block']
    reduce_key_pattern = ['header']
    map_udf = pandas_udf(
        lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, labeldf, sample_index),
        normal_eqn_struct, PandasUDFType.GROUPED_MAP)
    reduce_udf = pandas_udf(lambda key, pdf: reduce_normal_eqn(pdf), normal_eqn_struct,
                            PandasUDFType.GROUPED_MAP)

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

    XtX_out = X_out.values.T @ X_out.values
    XtY_out = X_out.values.T @ Y_out.values
    B = np.column_stack(
        [(np.linalg.inv(XtX_out + np.identity(XtX_out.shape[1]) * a) @ XtY_out) for a in alphas])

    sample_index = {
        r.sample_block: r.sample_ids
        for r in indexdf.select('sample_block', 'sample_ids').collect()
    }
    map_key_pattern = ['header_block', 'sample_block']
    reduce_key_pattern = ['header']
    map_udf = pandas_udf(
        lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, labeldf, sample_index),
        normal_eqn_struct, PandasUDFType.GROUPED_MAP)
    reduce_udf = pandas_udf(lambda key, pdf: reduce_normal_eqn(pdf), normal_eqn_struct,
                            PandasUDFType.GROUPED_MAP)
    model_udf = pandas_udf(
        lambda key, pdf: solve_normal_eqn(key, map_key_pattern, pdf, labeldf, alphaMap),
        model_struct, PandasUDFType.GROUPED_MAP)

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

    B_lvl = np.row_stack(outdf['coefficients'].values)

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

    XtX_out = X_out.values.T @ X_out.values
    XtY_out = X_out.values.T @ Y_out.values
    B = np.column_stack(
        [(np.linalg.inv(XtX_out + np.identity(XtX_out.shape[1]) * a) @ XtY_out) for a in alphas])
    X1_in = X_in.values @ B

    sample_index = {
        r.sample_block: r.sample_ids
        for r in indexdf.select('sample_block', 'sample_ids').collect()
    }
    map_key_pattern = ['header_block', 'sample_block']
    reduce_key_pattern = ['header']
    transform_key_pattern = ['header_block', 'sample_block']
    map_udf = pandas_udf(
        lambda key, pdf: map_normal_eqn(key, map_key_pattern, pdf, labeldf, sample_index),
        normal_eqn_struct, PandasUDFType.GROUPED_MAP)
    reduce_udf = pandas_udf(lambda key, pdf: reduce_normal_eqn(pdf), normal_eqn_struct,
                            PandasUDFType.GROUPED_MAP)
    model_udf = pandas_udf(
        lambda key, pdf: solve_normal_eqn(key, map_key_pattern, pdf, labeldf, alphaMap),
        model_struct, PandasUDFType.GROUPED_MAP)
    transform_udf = pandas_udf(
        lambda key, pdf: apply_model(key, transform_key_pattern, pdf, labeldf, alphaMap),
        reduced_matrix_struct, PandasUDFType.GROUPED_MAP)

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
    Y_out = labeldf.drop(ids, axis='rows')

    XtX_out = X_out.values.T @ X_out.values
    XtY_out = X_out.values.T @ Y_out.values
    B = np.column_stack(
        [(np.linalg.inv(XtX_out + np.identity(XtX_out.shape[1]) * a) @ XtY_out) for a in alphas])

    stack = RidgeReducer(alphas)
    modeldf = stack.fit(blockdf, labeldf, indexdf)

    columns = ['coefficients']
    rows = modeldf.filter(f'header_block = "{testBlock}" AND sample_block = {testGroup}') \
        .select(*columns).collect()
    outdf = pd.DataFrame(rows, columns=columns)

    B_stack = np.row_stack(outdf['coefficients'].values)

    assert np.allclose(B_stack, B)


def test_ridge_reducer_transform(spark):

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

    XtX_out = X_out.values.T @ X_out.values
    XtY_out = X_out.values.T @ Y_out.values
    B = np.column_stack(
        [(np.linalg.inv(XtX_out + np.identity(XtX_out.shape[1]) * a) @ XtY_out) for a in alphas])
    X1_in = X_in.values @ B

    stack = RidgeReducer(alphas)
    modeldf = stack.fit(blockdf, labeldf, indexdf)
    level1df = stack.transform(blockdf, labeldf, modeldf)

    columns = ['values']
    rows = level1df.filter(f'header LIKE "%{testBlock}%" AND sample_block = {testGroup}') \
        .select(*columns) \
        .collect()
    outdf = pd.DataFrame(rows, columns=columns)
    X1_in_stack = np.column_stack(outdf['values'])

    assert np.allclose(X1_in_stack, X1_in)


def test_one_level_regression(spark):

    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet')
    testLabel = 'sim100'

    group2ids = {
        r.sample_block: r.sample_ids
        for r in indexdf.select('sample_block', 'sample_ids').collect()
    }
    groups = sorted(group2ids.keys(), key=lambda v: v)
    headersToKeep = [c for c in X1.columns if testLabel in c]

    r2s = []
    for group in groups:
        ids = group2ids[group]
        X1_in = X1[headersToKeep].loc[ids, :].values
        X1_out = X1[headersToKeep].drop(ids, axis='rows')
        Y_in = labeldf[testLabel].loc[ids].values
        Y_out = labeldf[testLabel].loc[X1_out.index].values
        X1tX1_out = X1_out.values.T @ X1_out.values
        X1tY_out = X1_out.values.T @ Y_out
        B = np.column_stack(
            [(np.linalg.inv(X1tX1_out + np.identity(X1tX1_out.shape[1]) * a) @ X1tY_out)
             for a in alphas])
        X1B = X1_in @ B
        r2 = r_squared(X1B, Y_in.reshape(-1, 1))
        r2s.append(r2)

    r2_mean = np.row_stack(r2s).mean(axis=0)
    bestAlpha, bestr2 = sorted(zip(alphaMap.keys(), r2_mean), key=lambda t: -t[1])[0]

    y_hat = []
    r2s_pred = []
    for group in groups:
        ids = group2ids[group]
        X1_in = X1[headersToKeep].loc[ids, :].values
        X1_out = X1[headersToKeep].drop(ids, axis='rows')
        Y_in = labeldf[testLabel].loc[ids].values
        Y_out = labeldf[testLabel].loc[X1_out.index].values
        X1tX1_out = X1_out.values.T @ X1_out.values
        X1tY_out = X1_out.values.T @ Y_out
        b = np.linalg.inv(X1tX1_out +
                          np.identity(X1tX1_out.shape[1]) * alphaMap[bestAlpha]) @ X1tY_out
        r2s_pred.append(r_squared(X1_in @ b, Y_in))
        y_hat.extend((X1_in @ b).tolist())

    y_hat = np.array(y_hat)

    stack0 = RidgeReducer(alphas)
    model0df = stack0.fit(blockdf, labeldf, indexdf)
    level1df = stack0.transform(blockdf, labeldf, model0df)

    regressor = RidgeRegression(alphas)
    model1df, cvdf = regressor.fit(level1df, labeldf, indexdf)
    yhatdf = regressor.transform(level1df, labeldf, model1df, cvdf)

    r = cvdf.filter(f'label = "{testLabel}"').select('alpha', 'r2_mean').head()
    bestAlpha_lvl, bestr2_lvl = (r.alpha, r.r2_mean)
    y_hat_lvl = np.concatenate([
        r.values for r in yhatdf.filter(f'label = "{testLabel}"').orderBy('sample_block').select(
            'values').collect()
    ])

    assert (bestAlpha_lvl == bestAlpha and np.isclose(bestr2_lvl, bestr2) and
            np.allclose(y_hat_lvl, np.array(y_hat)))


def test_two_level_regression(spark):

    indexdf = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet')
    blockdf = spark.read.parquet(f'{data_root}/blockedGT.snappy.parquet')
    testLabel = 'sim100'
    columnIndexer = sorted(enumerate(alphaMap.keys()), key=lambda t: t[1])
    coefOrder = [i for i, a in columnIndexer]

    group2ids = {
        r.sample_block: r.sample_ids
        for r in indexdf.select('sample_block', 'sample_ids').collect()
    }
    groups = sorted(group2ids.keys(), key=lambda v: v)
    headersToKeep = [c for c in X2.columns if testLabel in c]

    r2s = []

    for group in groups:
        ids = group2ids[group]
        X2_in = X2[headersToKeep].loc[ids, :].values
        X2_out = X2[headersToKeep].drop(ids, axis='rows')
        Y_in = labeldf[testLabel].loc[ids].values
        Y_out = labeldf[testLabel].loc[X2_out.index].values
        X2tX2_out = X2_out.values.T @ X2_out.values
        X2tY_out = X2_out.values.T @ Y_out
        B = np.column_stack(
            [(np.linalg.inv(X2tX2_out + np.identity(X2tX2_out.shape[1]) * a) @ X2tY_out)
             for a in alphas])[:, coefOrder]
        X2B = X2_in @ B
        r2 = r_squared(X2B, Y_in.reshape(-1, 1))
        r2s.append(r2)

    r2_mean = np.row_stack(r2s).mean(axis=0)
    bestAlpha, bestr2 = sorted(zip(alphaMap.keys(), r2_mean), key=lambda t: -t[1])[0]

    y_hat = []
    r2s_pred = []

    for group in groups:
        ids = group2ids[group]
        X2_in = X2[headersToKeep].loc[ids, :].values
        X2_out = X2[headersToKeep].drop(ids, axis='rows')
        Y_in = labeldf[testLabel].loc[ids].values
        Y_out = labeldf[testLabel].loc[X2_out.index].values
        X2tX2_out = X2_out.values.T @ X2_out.values
        X2tY_out = X2_out.values.T @ Y_out
        b = np.linalg.inv(X2tX2_out +
                          np.identity(X2tX2_out.shape[1]) * alphaMap[bestAlpha]) @ X2tY_out
        r2s_pred.append(r_squared(X2_in @ b, Y_in))
        y_hat.extend((X2_in @ b).tolist())

    y_hat = np.array(y_hat)

    stack0 = RidgeReducer(alphas)
    model0df = stack0.fit(blockdf, labeldf, indexdf)
    level1df = stack0.transform(blockdf, labeldf, model0df)

    stack1 = RidgeReducer(alphas)
    model1df = stack1.fit(level1df, labeldf, indexdf)
    level2df = stack1.transform(level1df, labeldf, model1df)

    regressor = RidgeRegression(alphas)
    model2df, cvdf = regressor.fit(level2df, labeldf, indexdf)
    yhatdf = regressor.transform(level2df, labeldf, model2df, cvdf)

    r = cvdf.filter(f'label = "{testLabel}"').select('alpha', 'r2_mean').head()
    bestAlpha_lvl, bestr2_lvl = (r.alpha, r.r2_mean)
    y_hat_lvl = np.concatenate([
        r.values for r in yhatdf.filter(f'label = "{testLabel}"').orderBy('sample_block').select(
            'values').collect()
    ])

    assert (bestAlpha_lvl == bestAlpha and np.isclose(bestr2_lvl, bestr2) and
            np.allclose(y_hat_lvl, np.array(y_hat)))
