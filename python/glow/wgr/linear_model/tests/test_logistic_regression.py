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
from glow.wgr.linear_model.logistic_udfs import *
from glow.wgr.linear_model.logistic_model import *
import json
import math
import pytest

data_root = 'test-data/wgr/logistic-regression'

labeldf = pd.read_csv(f'{data_root}/binary_phenotypes.csv').set_index('sample_id')
labeldf.index = labeldf.index.astype(str, copy=False)
covdf = pd.read_csv(f'{data_root}/cov.csv').set_index('sample_id')
covdf.index = covdf.index.astype(str, copy=False)
covdf.insert(0, 'intercept', 1)
alpha_values = np.array([10, 30, 100])
alphas = {f'alpha_{i}': a for i, a in enumerate(alpha_values)}
test_sample_block = '1'
test_label = 'sim50'
test_alpha = 'alpha_0'


def test_map_irls_eqn(spark):
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}
    with open(f'{data_root}/test_map_irls_eqn.json') as json_file:
        test_values = json.load(json_file)
    map_key_pattern = ['sample_block', 'label', 'alpha_name']
    p0_dict = {k: v for k, v in zip(labeldf.columns, labeldf.sum(axis=0) / labeldf.shape[0])}
    map_udf = pandas_udf(
        lambda key, pdf: map_irls_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks, covdf,
                                      p0_dict, alphas), irls_eqn_struct, PandasUDFType.GROUPED_MAP)

    mapdf = lvl1df \
        .withColumn('alpha_name', f.explode(f.array([f.lit(n) for n in alphas.keys()]))) \
        .groupBy(map_key_pattern) \
        .apply(map_udf)

    outdf = mapdf.filter(
        f'sample_block = "{test_sample_block}" AND label = "{test_label}" AND alpha_name = "{test_alpha}"'
    ).toPandas()
    xtgx_glow = np.row_stack(outdf['xtgx'])
    xty_glow = outdf['xty'].ravel()
    beta_glow = outdf['beta'].ravel()

    assert (np.allclose(np.array(test_values['xtgx']), xtgx_glow) and
            np.allclose(np.array(test_values['xty']), xty_glow) and
            np.allclose(np.array(test_values['beta']), beta_glow))


def test_reduce_irls_eqn(spark):
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}
    with open(f'{data_root}/test_reduce_irls_eqn.json') as json_file:
        test_values = json.load(json_file)

    map_key_pattern = ['sample_block', 'label', 'alpha_name']
    reduce_key_pattern = ['header_block', 'header', 'label', 'alpha_name']
    p0_dict = {k: v for k, v in zip(labeldf.columns, labeldf.sum(axis=0) / labeldf.shape[0])}

    map_udf = pandas_udf(
        lambda key, pdf: map_irls_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks, covdf,
                                      p0_dict, alphas), irls_eqn_struct, PandasUDFType.GROUPED_MAP)

    reduce_udf = pandas_udf(lambda key, pdf: reduce_irls_eqn(key, reduce_key_pattern, pdf),
                            irls_eqn_struct, PandasUDFType.GROUPED_MAP)

    reducedf = lvl1df \
        .withColumn('alpha_name', f.explode(f.array([f.lit(n) for n in alphas.keys()]))) \
        .groupBy(map_key_pattern) \
        .apply(map_udf) \
        .groupBy(reduce_key_pattern) \
        .apply(reduce_udf)

    outdf = reducedf.filter(f'sample_block = "{test_sample_block}" AND label = "{test_label}" AND alpha_name = "{test_alpha}"') \
        .orderBy('sort_key', 'header') \
        .toPandas()

    xtgx_glow = np.row_stack(outdf['xtgx'])
    xty_glow = outdf['xty'].ravel()
    beta_glow = outdf['beta'].ravel()

    assert (np.allclose(np.array(test_values['xtgx']), xtgx_glow) and
            np.allclose(np.array(test_values['xty']), xty_glow) and
            np.allclose(np.array(test_values['beta']), beta_glow))


def test_solve_irls_eqn(spark):
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}
    with open(f'{data_root}/test_solve_irls_eqn.json') as json_file:
        test_values = json.load(json_file)

    map_key_pattern = ['sample_block', 'label', 'alpha_name']
    reduce_key_pattern = ['header_block', 'header', 'label', 'alpha_name']
    model_key_pattern = ['sample_block', 'label']
    p0_dict = {k: v for k, v in zip(labeldf.columns, labeldf.sum(axis=0) / labeldf.shape[0])}

    map_udf = pandas_udf(
        lambda key, pdf: map_irls_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks, covdf,
                                      p0_dict, alphas), irls_eqn_struct, PandasUDFType.GROUPED_MAP)

    reduce_udf = pandas_udf(lambda key, pdf: reduce_irls_eqn(key, reduce_key_pattern, pdf),
                            irls_eqn_struct, PandasUDFType.GROUPED_MAP)

    model_udf = pandas_udf(
        lambda key, pdf: solve_irls_eqn(key, model_key_pattern, pdf, labeldf, alphas), model_struct,
        PandasUDFType.GROUPED_MAP)

    modeldf = lvl1df \
        .withColumn('alpha_name', f.explode(f.array([f.lit(n) for n in alphas.keys()]))) \
        .groupBy(map_key_pattern) \
        .apply(map_udf) \
        .groupBy(reduce_key_pattern) \
        .apply(reduce_udf) \
        .groupBy(model_key_pattern) \
        .apply(model_udf)

    outdf = modeldf.filter(f'sample_block = "{test_sample_block}"') \
        .filter(f.col('labels').getItem(0) == test_label) \
        .orderBy('sort_key', 'header') \
        .toPandas()

    betas_glow = np.row_stack(outdf['coefficients'])

    assert (np.allclose(np.array(test_values['betas']), betas_glow))


def test_score_logistic_model(spark):
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}
    with open(f'{data_root}/test_score_logistic_model.json') as json_file:
        test_values = json.load(json_file)

    map_key_pattern = ['sample_block', 'label', 'alpha_name']
    reduce_key_pattern = ['header_block', 'header', 'label', 'alpha_name']
    model_key_pattern = ['sample_block', 'label']
    p0_dict = {k: v for k, v in zip(labeldf.columns, labeldf.sum(axis=0) / labeldf.shape[0])}

    map_udf = pandas_udf(
        lambda key, pdf: map_irls_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks, covdf,
                                      p0_dict, alphas), irls_eqn_struct, PandasUDFType.GROUPED_MAP)

    reduce_udf = pandas_udf(lambda key, pdf: reduce_irls_eqn(key, reduce_key_pattern, pdf),
                            irls_eqn_struct, PandasUDFType.GROUPED_MAP)

    model_udf = pandas_udf(
        lambda key, pdf: solve_irls_eqn(key, model_key_pattern, pdf, labeldf, alphas), model_struct,
        PandasUDFType.GROUPED_MAP)

    score_udf = pandas_udf(
        lambda key, pdf: score_models(
            key, model_key_pattern, pdf, labeldf, sample_blocks, alphas, covdf, metric='log_loss'),
        cv_struct, PandasUDFType.GROUPED_MAP)

    modeldf = lvl1df \
        .withColumn('alpha_name', f.explode(f.array([f.lit(n) for n in alphas.keys()]))) \
        .groupBy(map_key_pattern) \
        .apply(map_udf) \
        .groupBy(reduce_key_pattern) \
        .apply(reduce_udf) \
        .groupBy(model_key_pattern) \
        .apply(model_udf)

    cvdf = lvl1df.drop('header_block', 'sort_key') \
        .join(modeldf, ['header', 'sample_block'], 'right') \
        .withColumn('label', f.coalesce(f.col('label'), f.col('labels').getItem(0))) \
        .groupBy(model_key_pattern) \
        .apply(score_udf)

    outdf = cvdf.filter(
        f'sample_block = "{test_sample_block}" AND label = "{test_label}"').toPandas()
    scores_glow = outdf['score'].to_numpy()

    assert (np.allclose(np.array(test_values['scores']), scores_glow))


def test_logistic_regression_fit(spark):
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}
    with open(f'{data_root}/test_logistic_regression_fit.json') as json_file:
        test_values = json.load(json_file)

    logreg = LogisticRegression(alpha_values)
    modeldf, cvdf = logreg.fit(lvl1df, labeldf, sample_blocks, covdf)

    outdf = cvdf.filter(f'label = "{test_label}"').toPandas()

    best_score_glow = outdf['log_loss_mean'].to_numpy()[0]
    best_alpha_glow = outdf['alpha'].to_numpy()[0]

    assert (np.isclose(test_values['best_score'], best_score_glow) and
            test_values['best_alpha'] == best_alpha_glow)


def test_logistic_regression_transform(spark):
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}
    with open(f'{data_root}/test_logistic_regression_transform.json') as json_file:
        test_values = json.load(json_file)

    logreg = LogisticRegression(alpha_values)
    modeldf, cvdf = logreg.fit(lvl1df, labeldf, sample_blocks, covdf)
    wgr_cov_df = logreg.transform(lvl1df, labeldf, sample_blocks, modeldf, cvdf)
    wgr_cov_glow = wgr_cov_df[test_label].to_numpy()

    assert (np.allclose(np.array(test_values['wgr_cov']), wgr_cov_glow))


def test_logistic_regression_predict_proba(spark):
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}
    with open(f'{data_root}/test_logistic_regression_predict_proba.json') as json_file:
        test_values = json.load(json_file)

    logreg = LogisticRegression(alpha_values)
    modeldf, cvdf = logreg.fit(lvl1df, labeldf, sample_blocks, covdf)
    prob_df = logreg.predict_proba(lvl1df, labeldf, sample_blocks, modeldf, cvdf, covdf)
    prob_glow = prob_df[test_label].to_numpy()

    assert (np.allclose(np.array(test_values['prob']), prob_glow))


def test_logistic_regression_fit_transform(spark):
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}

    logreg = LogisticRegression(alpha_values)
    modeldf, cvdf = logreg.fit(lvl1df, labeldf, sample_blocks, covdf)
    wgr_cov_df0 = logreg.transform(lvl1df, labeldf, sample_blocks, modeldf, cvdf)
    wgr_cov_df1 = logreg.fit_transform(lvl1df, labeldf, sample_blocks, covdf)
    wgr_cov0 = wgr_cov_df0[test_label].to_numpy()
    wgr_cov1 = wgr_cov_df1[test_label].to_numpy()

    assert (np.allclose(wgr_cov0, wgr_cov1))
