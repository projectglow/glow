from glow.wgr.linear_model.functions import *
import numpy as np
import pandas as pd
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
