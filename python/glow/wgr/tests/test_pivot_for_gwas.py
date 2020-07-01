import numpy as np
import pandas as pd
import pytest
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException
import glow
from glow.wgr import functions


def test_error_too_many_levels(spark):
    df = pd.DataFrame(columns=['c1', 'c2', 'c3'])
    df = df.set_index(['c1', 'c2', 'c3'])
    with pytest.raises(ValueError):
        functions.pivot_for_gwas(spark, df)


def test_pivot_loco(spark):
    df = pd.read_csv('test-data/wgr/ridge-regression/level1YHatLoco.csv')
    df = df.set_index(['sample_id', 'contigName'])
    pivoted = functions.pivot_for_gwas(spark, df)
    assert pivoted.count() == 12
    assert pivoted.columns == ['label', 'contigName', 'values']
    pt_list = pivoted.where('label = "sim100" and contigName = 1').head().values
    assert len(pt_list) == 100
    expected = [
        0.2696910984588018, -0.5467797324110817, 1.6258456800754162, 0.03999127669745301,
        1.093584890813106, -0.5203782912574525, -0.6729752808261074, -0.3071464501504048,
        -0.016041461027687984, 0.3213354383996257
    ]
    assert np.allclose(expected, pt_list[:10])


def test_pivot_no_loco(spark):
    df = pd.read_csv('test-data/wgr/ridge-regression/pts.csv')
    df = df.set_index('sample_id')
    pivoted = functions.pivot_for_gwas(spark, df)
    assert pivoted.count() == 4
    assert pivoted.columns == ['label', 'values']
    pt_list = pivoted.where('label = "sim100"').head().values
    assert len(pt_list) == 100
    expected = [
        -0.7912337651829717, -0.5390009590057597, -0.8270391986299384, -1.0100705379416486,
        -0.8959944804741121, 0.7842734642411621, -1.9412983640095125, 0.29875751192279376,
        -0.5337275979316131, -0.955664953613764
    ]
    assert np.allclose(expected, pt_list[:10])
