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

from glow.conversions import OneDimensionalDoubleNumpyArrayConverter, TwoDimensionalDoubleNumpyArrayConverter
from importlib import reload
import numpy as np
from py4j.protocol import Py4JJavaError
from pyspark.ml.linalg import DenseMatrix
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import pytest


def test_convert_matrix(spark):
    str_list = ['a', 'b']
    df = spark.createDataFrame(str_list, StringType())
    ndarray = np.array([[1.0, 2.1, 3.2], [4.3, 5.4, 6.5]])
    output_rows = df.withColumn("matrix", lit(ndarray)).collect()
    expected_matrix = DenseMatrix(2, 3, [1.0, 4.3, 2.1, 5.4, 3.2, 6.5])
    assert (output_rows[0].matrix == expected_matrix)
    assert (output_rows[1].matrix == expected_matrix)


def test_convert_array(spark):
    str_list = ['a', 'b']
    df = spark.createDataFrame(str_list, StringType())
    ndarray = np.array([1.0, 2.1, 3.2])
    output_rows = df.withColumn("array", lit(ndarray)).collect()
    expected_array = [1.0, 2.1, 3.2]
    assert (output_rows[0].array == expected_array)
    assert (output_rows[1].array == expected_array)


def test_convert_checks_dimension(spark):
    # No support for 3-dimensional arrays
    ndarray = np.array([[[1.]]])
    with pytest.raises(Py4JJavaError):
        lit(ndarray)


def test_convert_matrix_checks_type(spark):
    ndarray = np.array([[1, 2], [3, 4]])
    with pytest.raises(AttributeError):
        lit(ndarray)


def test_convert_array_checks_type(spark):
    ndarray = np.array([1, 2])
    with pytest.raises(AttributeError):
        lit(ndarray)


def test_register_converters_idempotent(spark):
    import glow.glow
    for _ in range(3):
        reload(glow.glow)
        one_d_converters = 0
        two_d_converters = 0
        for c in spark._sc._gateway._gateway_client.converters:
            if type(c) is OneDimensionalDoubleNumpyArrayConverter:
                one_d_converters += 1
            if type(c) is TwoDimensionalDoubleNumpyArrayConverter:
                two_d_converters += 1
        assert (one_d_converters == 1)
        assert (two_d_converters == 1)
