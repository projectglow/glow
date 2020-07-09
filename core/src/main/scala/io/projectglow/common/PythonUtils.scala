package io.projectglow.common

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

import org.apache.spark.ml.linalg.DenseMatrix

object PythonUtils {
  def doubleArrayFromBytes(len: Int, bytes: Array[Byte]): Array[Double] = {
    val buffer = ByteBuffer.wrap(bytes)
    val out = new Array[Double](len)
    var i = 0
    while (i < len) {
      out(i) = buffer.getDouble()
      i += 1
    }
    out
  }
}
