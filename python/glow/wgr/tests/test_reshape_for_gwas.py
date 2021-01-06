import numpy as np
import pandas as pd
import pytest
import glow


def test_error_too_many_levels(spark):
    df = pd.DataFrame(columns=['c1', 'c2', 'c3']).set_index(['c1', 'c2', 'c3'])
    with pytest.raises(ValueError):
        glow.wgr.reshape_for_gwas(spark, df)


def test_reshape_loco(spark):
    df = pd.read_csv('test-data/wgr/ridge-regression/level1YHatLoco.csv').set_index(
        ['sample_id', 'contigName'])
    reshaped = glow.wgr.reshape_for_gwas(spark, df)
    assert reshaped.count() == 12
    assert reshaped.columns == ['contigName', 'label', 'values']
    pt_list = reshaped.where('label = "sim100" and contigName = 1').head().values
    expected = df.xs(1, level='contigName')['sim100'].to_numpy()
    assert np.allclose(expected, np.array(pt_list))


def test_reshape_loco_string_contig(spark):
    df = pd.read_csv('test-data/wgr/ridge-regression/level1YHatLoco.csv',
                     dtype={
                         'contigName': 'str'
                     }).set_index(['sample_id', 'contigName'])
    reshaped = glow.wgr.reshape_for_gwas(spark, df)
    assert reshaped.count() == 12
    assert reshaped.columns == ['contigName', 'label', 'values']
    pt_list = reshaped.where('label = "sim100" and contigName = "1"').head().values
    expected = df.xs('1', level='contigName')['sim100'].to_numpy()
    assert np.allclose(expected, np.array(pt_list))


def test_reshape_no_loco(spark):
    df = pd.read_csv('test-data/wgr/ridge-regression/pts.csv').set_index('sample_id')
    reshaped = glow.wgr.reshape_for_gwas(spark, df)
    assert reshaped.count() == 4
    assert reshaped.columns == ['label', 'values']
    pt_list = reshaped.where('label = "sim100"').head().values
    expected = df['sim100']
    assert np.allclose(expected, pt_list)
