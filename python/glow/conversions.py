import numpy as np
from py4j.protocol import register_input_converter
from pyspark import SparkContext

class NdarrayConverter(object):
    """
    Replaces any 2-dimensional numpy array of doubles with a DenseMatrix.

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
        return isinstance(object, np.ndarray) and len(object.shape) == 2 and object.dtype.type == np.double

    def convert(self, object, gateway_client):
        sc = SparkContext._active_spark_context
        flat_arr = object.ravel()
        java_arr = sc._gateway.new_array(sc._jvm.double, flat_arr.shape[0])
        for idx, ele in enumerate(flat_arr):
            java_arr[idx] = ele.item()
        dense_matrix = sc._jvm.org.apache.spark.ml.linalg.DenseMatrix(object.shape[0], object.shape[1], java_arr)
        matrix_udt = sc._jvm.org.apache.spark.ml.linalg.MatrixUDT()
        converter = sc._jvm.org.apache.spark.sql.catalyst.CatalystTypeConverters.createToCatalystConverter(matrix_udt)
        literal_matrix = sc._jvm.org.apache.spark.sql.catalyst.expressions.Literal.create(
            converter.apply(dense_matrix), matrix_udt)
        return literal_matrix

register_input_converter(NdarrayConverter(), prepend=True)
