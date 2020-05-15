from numpy import ndarray

def wrapOneDimensionalNpArray(arr):
    gateway = spark.sparkContext._gateway
    jArr = gateway.new_array(gateway.jvm.double, arr.shape[0])
    for idx, ele in enumerate(arr):
        jArr[idx] = ele.item()
    return jArr

def wrapTwoDimensionalNpArray(ndArr):
    flatArr = wrapOneDimensionalNpArray(ndArr.ravel())
    mat = gateway.jvm.org.apache.spark.ml.linalg.DenseMatrix(ndArr.shape[0], ndArr.shape[1], flatArr)
    return mat

py4jMat = wrapTwoDimensionalNpArray(ndarray)

class NdarrayConverter(object):
    def can_convert(self, object):
        return isinstance(object, ndarray)

    def convert(self, object, gateway_client):
        Literal = JavaClass("org.apache.spark.sql.catalyst.expressions.Literal", gateway_client)
        java_set = Literal.create()
        for element in object:
            java_set.add(element)
        return java_set



matrixUdt = gateway.jvm.org.apache.spark.ml.linalg.MatrixUDT()
converter = gateway.jvm.org.apache.spark.sql.catalyst.CatalystTypeConverters.createToCatalystConverter(matrixUdt)
literalMatrix = gateway.jvm.org.apache.spark.sql.catalyst.expressions.Literal.create(converter.apply(py4jMat), matrixUdt)
