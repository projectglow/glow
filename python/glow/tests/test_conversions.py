import numpy as np
from py4j.protocol import Py4JJavaError
from pyspark.ml.linalg import DenseMatrix
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import pytest


def test_convert(spark):
    str_list = ['a', 'b']
    df = spark.createDataFrame(str_list, StringType())
    ndarray = np.array([[1.0, 2.1, 3.2], [4.3, 5.4, 6.5]])
    output_rows = df.withColumn("matrix", lit(ndarray)).collect()
    expected_matrix = DenseMatrix(2, 3, [1.0, 2.1, 3.2, 4.3, 5.4, 6.5])
    assert(output_rows[0].matrix == expected_matrix)
    assert(output_rows[1].matrix == expected_matrix)


def test_convert_checks_dimension(spark):
    ndarray = np.array([[[1.]]])
    with pytest.raises(Py4JJavaError):
        lit(ndarray)


def test_convert_checks_type(spark):
    ndarray = np.array([[1, 2], [3, 4]])
    with pytest.raises(AttributeError):
        lit(ndarray)
