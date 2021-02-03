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

from glow.wgr.model_functions import *
from glow.wgr.model_functions import _prepare_covariates, _prepare_labels_and_warn
import math
import numpy as np
import pandas as pd
from pyspark.sql import Row
import pytest
from sklearn.linear_model import LogisticRegression
from sklearn.datasets import load_breast_cancer


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
    block = assemble_block(n_rows=2,
                           n_cols=1,
                           pdf=df,
                           cov_matrix=np.array([]),
                           row_mask=np.array([]))
    assert np.allclose(block, np.array([[-1.], [1.]]))


def test_assemble_block_zero_sig():
    df = pd.DataFrame({'mu': [0.2, 0], 'sig': [0.1, 0], 'values': [[0.1, 0.3], [0, 0]]})
    with pytest.raises(ValueError):
        assemble_block(n_rows=2, n_cols=2, pdf=df, cov_matrix=np.array([]), row_mask=np.array([]))


def test_generate_alphas(spark):
    df = spark.createDataFrame([
        Row(header='chr_3_block_8_alpha_0_label_sim1'),
        Row(header='chr_3_block_8_alpha_0_label_sim2'),
        Row(header='chr_3_block_8_alpha_1'),
        Row(header='chr_3_block_8_alpha_1_label_sim1')
    ])
    expected_alphas = {
        'alpha_0': np.float(2 / 0.99),
        'alpha_1': np.float(2 / 0.75),
        'alpha_2': np.float(2 / 0.5),
        'alpha_3': np.float(2 / 0.25),
        'alpha_4': np.float(2 / 0.01)
    }
    assert generate_alphas(df) == expected_alphas


def test_prepare_covariates():
    label_df = pd.DataFrame({'Trait_1': [0, -1, 1], 'Trait_2': [0, 1, math.nan]})
    cov_df = pd.DataFrame({'Covariate_1': [0, 2, -1], 'Covariate_2': [0, -1, 1]})
    expected_df = pd.DataFrame({
        'intercept': [1, 1, 1],
        'Covariate_1': [-0.218218, 1.091089, -0.872872],
        'Covariate_2': [0, -1, 1]
    })
    assert (np.allclose(_prepare_covariates(cov_df, label_df, True), expected_df))
    assert (np.allclose(_prepare_covariates(cov_df, label_df, False),
                        expected_df.drop(columns=['intercept']), 1e-5))


def test_prepare_covariates_insufficient_elements():
    label_df = pd.DataFrame({'Trait_1': [0, -1, 1], 'Trait_2': [0, 1, math.nan]})
    cov_df = pd.DataFrame({'Covariate_1': [0, 2], 'Covariate_2': [0, -1]})
    with pytest.raises(
            ValueError,
            match=r'.*cov_df must be either empty of have the same number of rows as label_df.*'):
        _prepare_covariates(cov_df, label_df, True)


def test_prepare_labels_and_warn(capfd):
    messages = [
        "The label DataFrame is binary. Reduction/regression for binary phenotypes will be applied.\n",
        "Reduction/regression for binary phenotypes will be applied.\n",
        "Reduction/regression for quantitative phenotypes will be applied.\n",
        "The label DataFrame is quantitative. Reduction/regression for quantitative phenotypes will be applied.\n"
    ]
    b_label_df = pd.DataFrame({'Trait_1': [0, 1, 1], 'Trait_2': [0, 1, math.nan]})
    q_label_df = pd.DataFrame({'Trait_1': [0, 1.5, 1], 'Trait_2': [0, 1, math.nan]})
    b_std_label_df = pd.DataFrame({
        'Trait_1': [-1.154701, 0.577350, 0.577350],
        'Trait_2': [-0.707107, 0.707107, 0]
    })
    q_std_label_df = pd.DataFrame({
        'Trait_1': [-1.091089, 0.872872, 0.218218],
        'Trait_2': [-0.707107, 0.707107, 0]
    })

    assert np.allclose(_prepare_labels_and_warn(b_label_df, True, 'detect'), b_std_label_df)
    assert capfd.readouterr().out == messages[0]
    assert np.allclose(_prepare_labels_and_warn(b_label_df, True, 'binary'), b_std_label_df)
    assert capfd.readouterr().out == messages[1]
    assert np.allclose(_prepare_labels_and_warn(b_label_df, True, 'quantitative'), b_std_label_df)
    assert capfd.readouterr().out == messages[2]
    assert np.allclose(_prepare_labels_and_warn(q_label_df, False, 'detect'), q_std_label_df)
    assert capfd.readouterr().out == messages[3]
    assert np.allclose(_prepare_labels_and_warn(q_label_df, False, 'quantitative'), q_std_label_df)
    assert capfd.readouterr().out == messages[2]

    with pytest.raises(TypeError, match='Binary label DataFrame expected!'):
        _prepare_labels_and_warn(q_label_df, False, 'binary')


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


def test_constrained_logistic_fit():
    X_raw, y = load_breast_cancer(return_X_y=True)
    mu, sig = X_raw.mean(axis=0), X_raw.std(axis=0)
    X = (X_raw - mu) / sig
    alphas = [10, 30, 100, 300, 1000, 3000]

    for a in alphas:
        model_skl = LogisticRegression(C=1 / a, fit_intercept=True)
        model_skl.fit(X, y)
        X_with_int = np.column_stack([np.ones(X.shape[0]), X])
        alpha_arr = np.zeros(X_with_int.shape[1])
        alpha_arr[1:] = a
        ridge_fit = constrained_logistic_fit(X_with_int, y, alpha_arr, guess=np.array([]), n_cov=0)
        beta_skl = np.concatenate([model_skl.intercept_, model_skl.coef_.ravel()])
        beta_glow = ridge_fit.x
        assert (np.allclose(beta_skl, beta_glow, 0.01))


def test_irls_one_step():
    def irls_fit(X, y, alpha, n_cov, beta_cov):
        if n_cov > 0:
            beta = np.zeros(X.shape[1])
            beta[:n_cov] = beta_cov
        else:
            beta = np.zeros(X.shape[1])

        z = X @ beta
        p = sigmoid(z)
        xtgx = (X.T * (p * (1 - p))) @ X
        xty = X.T @ (p - y)
        loss_old = log_loss(p.reshape(-1, 1), y.reshape(-1, 1))[0]

        data = {'xtgx': list(xtgx), 'xty': list(xty), 'beta': list(beta)}

        pdf = pd.DataFrame(data)

        dloss = loss_old * 10
        tol = 1E-5
        n_steps = 0
        while dloss > tol and n_steps <= 10000:
            beta = irls_one_step(pdf, alpha, n_cov)
            z = X @ beta
            p = sigmoid(z)
            xtgx = (X.T * (p * (1 - p))) @ X
            xty = X.T @ (p - y)
            loss = log_loss(p.reshape(-1, 1), y.reshape(-1, 1))[0]
            dloss = loss_old - loss
            loss_old = loss

            data = {'xtgx': list(xtgx), 'xty': list(xty), 'beta': list(beta)}

            pdf = pd.DataFrame(data)
            n_steps += 1

        return beta

    X_raw, y = load_breast_cancer(return_X_y=True)
    mu, sig = X_raw.mean(axis=0), X_raw.std(axis=0)
    X = (X_raw - mu) / sig
    alphas = [10, 30, 100, 300, 1000, 3000]
    for a in alphas:
        model_skl = LogisticRegression(C=1 / a, fit_intercept=True)
        model_skl.fit(X, y)
        X_with_int = np.column_stack([np.ones(X.shape[0]), X])
        beta_irls = irls_fit(X_with_int, y, a, 1, model_skl.intercept_)
        beta_skl = np.concatenate([model_skl.intercept_, model_skl.coef_.ravel()])
        assert (np.allclose(beta_irls, beta_skl, 0.01))
