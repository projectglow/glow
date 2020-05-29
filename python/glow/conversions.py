import numpy as np
from py4j.java_collections import JavaArray
from pyspark import SparkContext
from typeguard import check_argument_types, check_return_type


def _is_numpy_double_array(object, dimensions: int) -> bool:
    assert check_argument_types()
    output = isinstance(object, np.ndarray) and len(
        object.shape) == dimensions and object.dtype.type == np.double
    assert check_return_type(output)
    return output


def _convert_numpy_to_java_array(np_arr: np.ndarray) -> JavaArray:
    """
    Converts a flat numpy array of doubles to a Java array of doubles.
    """
    assert check_argument_types()
    assert len(np_arr.shape) == 1
    assert np_arr.dtype.type == np.double

    sc = SparkContext._active_spark_context
    java_arr = sc._gateway.new_array(sc._jvm.double, np_arr.shape[0])
    for idx, ele in enumerate(np_arr):
        java_arr[idx] = ele.item()

    assert check_return_type(java_arr)
    return java_arr


class OneDimensionalDoubleNumpyArrayConverter(object):
    """
    Replaces any 1-dimensional numpy array of doubles with a literal Java array.

    Added in version 0.4.0.

    Examples:
        >>> import numpy as np
        >>> from pyspark.sql.functions import lit
        >>> from pyspark.sql.types import StringType
        >>> str_list = ['a', 'b']
        >>> df = spark.createDataFrame(str_list, StringType())
        >>> ndarray = np.array([1.0, 2.1, 3.2])
        >>> df.withColumn("array", lit(ndarray)).collect()
        [Row(value='a', array=[1.0, 2.1, 3.2]), Row(value='b', array=[1.0, 2.1, 3.2])]
    """
    def can_convert(self, object):
        return _is_numpy_double_array(object, dimensions=1)

    def convert(self, object, gateway_client):
        sc = SparkContext._active_spark_context
        java_arr = _convert_numpy_to_java_array(object)
        return java_arr


class TwoDimensionalDoubleNumpyArrayConverter(object):
    """
    Replaces any 2-dimensional numpy array of doubles with a literal DenseMatrix.

    Added in version 0.4.0.

    Examples:
        >>> import numpy as np
        >>> from pyspark.sql.functions import lit
        >>> from pyspark.sql.types import StringType
        >>> str_list = ['a', 'b']
        >>> df = spark.createDataFrame(str_list, StringType())
        >>> ndarray = np.array([[1.0, 2.1, 3.2], [4.3, 5.4, 6.5]])
        >>> df.withColumn("matrix", lit(ndarray)).collect()
        [Row(value='a', matrix=DenseMatrix(2, 3, [1.0, 2.1, 3.2, 4.3, 5.4, 6.5], False)), Row(value='b', matrix=DenseMatrix(2, 3, [1.0, 2.1, 3.2, 4.3, 5.4, 6.5], False))]
    """
    def can_convert(self, object):
        return _is_numpy_double_array(object, dimensions=2)

    def convert(self, object, gateway_client):
        sc = SparkContext._active_spark_context
        flat_arr = object.ravel()
        java_arr = _convert_numpy_to_java_array(flat_arr)
        dense_matrix = sc._jvm.org.apache.spark.ml.linalg.DenseMatrix(object.shape[0],
                                                                      object.shape[1], java_arr)
        matrix_udt = sc._jvm.org.apache.spark.ml.linalg.MatrixUDT()
        converter = sc._jvm.org.apache.spark.sql.catalyst.CatalystTypeConverters.createToCatalystConverter(
            matrix_udt)
        literal_matrix = sc._jvm.org.apache.spark.sql.catalyst.expressions.Literal.create(
            converter.apply(dense_matrix), matrix_udt)
        return literal_matrix
