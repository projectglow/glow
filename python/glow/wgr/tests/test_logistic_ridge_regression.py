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

from pyspark.sql.functions import PandasUDFType
from glow.wgr.logistic_udfs import *
from glow.wgr.logistic_ridge_regression import *
from pyspark.sql import functions as f
import json
import pandas as pd
import numpy as np

data_root = 'test-data/wgr/logistic-regression'

labeldf = pd.read_csv(f'{data_root}/binary_phenotypes.csv').set_index('sample_id')
labeldf.index = labeldf.index.astype(str, copy=False)
covdf = pd.read_csv(f'{data_root}/cov.csv').set_index('sample_id')
covdf.index = covdf.index.astype(str, copy=False)
covdf.insert(0, 'intercept', 1)
maskdf = pd.DataFrame(data=np.where(np.isnan(labeldf), False, True),
                      columns=labeldf.columns,
                      index=labeldf.index)

beta_cov_dict = {}
for label in labeldf:
    row_mask = slice_label_rows(maskdf, label, list(covdf.index), np.array([], dtype='int')).ravel()
    cov_mat = slice_label_rows(covdf, 'all', list(covdf.index), row_mask)
    y = slice_label_rows(labeldf, label, list(labeldf.index), row_mask).ravel()
    fit_result = constrained_logistic_fit(cov_mat,
                                          y,
                                          np.zeros(cov_mat.shape[1]),
                                          guess=np.array([]),
                                          n_cov=0)
    beta_cov_dict[label] = fit_result.x

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
    map_udf = f.pandas_udf(
        lambda key, pdf: map_irls_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks, covdf,
                                      beta_cov_dict, maskdf, alphas), irls_eqn_struct,
        PandasUDFType.GROUPED_MAP)

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

    map_udf = f.pandas_udf(
        lambda key, pdf: map_irls_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks, covdf,
                                      beta_cov_dict, maskdf, alphas), irls_eqn_struct,
        PandasUDFType.GROUPED_MAP)

    reduce_udf = f.pandas_udf(lambda key, pdf: reduce_irls_eqn(key, reduce_key_pattern, pdf),
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
    model_key_pattern = ['sample_block', 'label', 'alpha_name']

    map_udf = f.pandas_udf(
        lambda key, pdf: map_irls_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks, covdf,
                                      beta_cov_dict, maskdf, alphas), irls_eqn_struct,
        PandasUDFType.GROUPED_MAP)

    reduce_udf = f.pandas_udf(lambda key, pdf: reduce_irls_eqn(key, reduce_key_pattern, pdf),
                              irls_eqn_struct, PandasUDFType.GROUPED_MAP)

    model_udf = f.pandas_udf(
        lambda key, pdf: solve_irls_eqn(key, model_key_pattern, pdf, labeldf, alphas, covdf),
        model_struct, PandasUDFType.GROUPED_MAP)

    modeldf = lvl1df \
        .withColumn('alpha_name', f.explode(f.array([f.lit(n) for n in alphas.keys()]))) \
        .groupBy(map_key_pattern) \
        .apply(map_udf) \
        .groupBy(reduce_key_pattern) \
        .apply(reduce_udf) \
        .groupBy(model_key_pattern) \
        .apply(model_udf) \
        .withColumn('alpha_label_coef', f.expr('struct(alphas[0] AS alpha, labels[0] AS label, coefficients[0] AS coefficient)')) \
        .groupBy('header_block', 'sample_block', 'header', 'sort_key', f.col('alpha_label_coef.label')) \
        .agg(f.sort_array(f.collect_list('alpha_label_coef')).alias('alphas_labels_coefs')) \
        .selectExpr('*', 'alphas_labels_coefs.alpha AS alphas', 'alphas_labels_coefs.label AS labels', 'alphas_labels_coefs.coefficient AS coefficients') \
        .drop('alphas_labels_coefs', 'label')

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
    model_key_pattern = ['sample_block', 'label', 'alpha_name']
    score_key_pattern = ['sample_block', 'label']

    map_udf = f.pandas_udf(
        lambda key, pdf: map_irls_eqn(key, map_key_pattern, pdf, labeldf, sample_blocks, covdf,
                                      beta_cov_dict, maskdf, alphas), irls_eqn_struct,
        PandasUDFType.GROUPED_MAP)

    reduce_udf = f.pandas_udf(lambda key, pdf: reduce_irls_eqn(key, reduce_key_pattern, pdf),
                              irls_eqn_struct, PandasUDFType.GROUPED_MAP)

    model_udf = f.pandas_udf(
        lambda key, pdf: solve_irls_eqn(key, model_key_pattern, pdf, labeldf, alphas, covdf),
        model_struct, PandasUDFType.GROUPED_MAP)

    score_udf = f.pandas_udf(
        lambda key, pdf: score_models(key,
                                      score_key_pattern,
                                      pdf,
                                      labeldf,
                                      sample_blocks,
                                      alphas,
                                      covdf,
                                      maskdf,
                                      metric='log_loss'), cv_struct, PandasUDFType.GROUPED_MAP)

    modeldf = lvl1df \
        .withColumn('alpha_name', f.explode(f.array([f.lit(n) for n in alphas.keys()]))) \
        .groupBy(map_key_pattern) \
        .apply(map_udf) \
        .groupBy(reduce_key_pattern) \
        .apply(reduce_udf) \
        .groupBy(model_key_pattern) \
        .apply(model_udf) \
        .withColumn('alpha_label_coef', f.expr('struct(alphas[0] AS alpha, labels[0] AS label, coefficients[0] AS coefficient)')) \
        .groupBy('header_block', 'sample_block', 'header', 'sort_key', f.col('alpha_label_coef.label')) \
        .agg(f.sort_array(f.collect_list('alpha_label_coef')).alias('alphas_labels_coefs')) \
        .selectExpr('*', 'alphas_labels_coefs.alpha AS alphas', 'alphas_labels_coefs.label AS labels', 'alphas_labels_coefs.coefficient AS coefficients') \
        .drop('alphas_labels_coefs', 'label')

    cvdf = lvl1df.drop('header_block', 'sort_key') \
        .join(modeldf, ['header', 'sample_block'], 'right') \
        .withColumn('label', f.coalesce(f.col('label'), f.col('labels').getItem(0))) \
        .groupBy(score_key_pattern) \
        .apply(score_udf)

    outdf = cvdf.filter(
        f'sample_block = "{test_sample_block}" AND label = "{test_label}"').toPandas()
    scores_glow = outdf['score'].to_numpy()

    assert (np.allclose(np.array(test_values['scores']), scores_glow))


def test_logistic_regression_fit(spark):
    covdf = pd.read_csv(f'{data_root}/cov.csv').set_index('sample_id')
    covdf.index = covdf.index.astype(str, copy=False)
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}
    with open(f'{data_root}/test_logistic_regression_fit.json') as json_file:
        test_values = json.load(json_file)

    logreg = LogisticRidgeRegression(lvl1df, labeldf, sample_blocks, covdf, alphas=alpha_values)
    modeldf, cvdf = logreg.fit()

    outdf = cvdf.filter(f'label = "{test_label}"').toPandas()

    best_score_glow = outdf['log_loss_mean'].to_numpy()[0]
    best_alpha_glow = outdf['alpha'].to_numpy()[0]

    assert (np.isclose(test_values['best_score'], best_score_glow) and
            test_values['best_alpha'] == best_alpha_glow)


def test_logistic_regression_transform(spark):
    covdf = pd.read_csv(f'{data_root}/cov.csv').set_index('sample_id')
    covdf.index = covdf.index.astype(str, copy=False)
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}
    with open(f'{data_root}/test_logistic_regression_transform.json') as json_file:
        test_values = json.load(json_file)

    logreg = LogisticRidgeRegression(lvl1df, labeldf, sample_blocks, covdf, alphas=alpha_values)
    logreg.fit()
    wgr_cov_df = logreg.transform()
    wgr_cov_glow = wgr_cov_df[test_label].to_numpy()

    assert (np.allclose(np.array(test_values['wgr_cov']), wgr_cov_glow))


def test_logistic_regression_predict_proba(spark):
    covdf = pd.read_csv(f'{data_root}/cov.csv').set_index('sample_id')
    covdf.index = covdf.index.astype(str, copy=False)
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}
    with open(f'{data_root}/test_logistic_regression_predict_proba.json') as json_file:
        test_values = json.load(json_file)

    logreg = LogisticRidgeRegression(lvl1df, labeldf, sample_blocks, covdf, alphas=alpha_values)
    logreg.fit()
    prob_df = logreg.transform(response='sigmoid')
    prob_glow = prob_df[test_label].to_numpy()

    assert (np.allclose(np.array(test_values['prob']), prob_glow))


def test_logistic_regression_fit_transform(spark):
    covdf = pd.read_csv(f'{data_root}/cov.csv').set_index('sample_id')
    covdf.index = covdf.index.astype(str, copy=False)
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}

    logreg = LogisticRidgeRegression(lvl1df, labeldf, sample_blocks, covdf, alphas=alpha_values)
    logreg.fit()
    wgr_cov_df0 = logreg.transform()
    wgr_cov_df1 = logreg.fit_transform()
    wgr_cov0 = wgr_cov_df0[test_label].to_numpy()
    wgr_cov1 = wgr_cov_df1[test_label].to_numpy()

    assert (np.allclose(wgr_cov0, wgr_cov1))


def test_logistic_regression_transform_loco(spark):
    covdf = pd.read_csv(f'{data_root}/cov.csv').set_index('sample_id')
    covdf.index = covdf.index.astype(str, copy=False)
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}

    logreg = LogisticRidgeRegression(lvl1df, labeldf, sample_blocks, covdf, alphas=alpha_values)
    modeldf, cvdf = logreg.fit()
    wgr_cov_loco1_df1 = logreg.transform_loco(chromosomes=['1'])
    modeldf_loco1 = modeldf.filter('header NOT LIKE "%chr_1%"')
    logreg.model_df = modeldf_loco1
    wgr_cov_loco1_df0 = logreg.transform()

    wgr_cov_loco1_0 = wgr_cov_loco1_df0[test_label].to_numpy()
    wgr_cov_loco1_1 = wgr_cov_loco1_df1[test_label].to_numpy()

    assert (np.allclose(wgr_cov_loco1_0, wgr_cov_loco1_1))


def test_model_cv_df(spark):
    covdf = pd.read_csv(f'{data_root}/cov.csv').set_index('sample_id')
    covdf.index = covdf.index.astype(str, copy=False)
    lvl1df = spark.read.parquet(f'{data_root}/bt_reduceded_1part.snappy.parquet')
    sample_blocks_df = spark.read.parquet(f'{data_root}/groupedIDs.snappy.parquet') \
        .withColumn('sample_block', f.col('sample_block').cast('string')) \
        .withColumn('sample_ids', f.expr('transform(sample_ids, v -> cast(v as string))'))
    sample_blocks = {r.sample_block: r.sample_ids for r in sample_blocks_df.collect()}

    logreg = LogisticRidgeRegression(lvl1df, labeldf, sample_blocks, covdf, alphas=alpha_values)

    model_df = spark.createDataFrame([('Alice', 1)])
    logreg.model_df = model_df

    cv_df = spark.createDataFrame([('Bob', 2)])
    logreg.cv_df = cv_df

    assert str(logreg.model_df.storageLevel) == 'Serialized 1x Replicated'
    logreg._cache_model_cv_df()
    assert str(logreg.model_df.storageLevel) == 'Disk Memory Deserialized 1x Replicated'
    logreg._unpersist_model_cv_df()
    assert str(logreg.model_df.storageLevel) == 'Serialized 1x Replicated'
