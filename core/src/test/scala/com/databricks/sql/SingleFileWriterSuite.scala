package com.databricks.sql

import com.databricks.hls.sql.HLSBaseTest

class SingleFileWriterSuite extends HLSBaseTest {

  def testRepartition(
      testName: String,
      inputByteArrays: Seq[Array[Byte]],
      expectedByteArrays: Array[Array[Byte]],
      minPartitionSizeB: Int): Unit = {

    test(testName) {
      val rdd = spark.sparkContext.parallelize(inputByteArrays, inputByteArrays.size)
      val repartitioned = SingleFileWriter.repartitionRddToUpload(rdd, minPartitionSizeB)
      assert(repartitioned.collect().deep == expectedByteArrays.deep)
    }
  }

  // Convenience functions to avoid typing toByte a million times
  def ba(bytes: Byte*): Array[Byte] = Array[Byte](bytes: _*)
  def baf(len: Int, byte: Byte = 0): Array[Byte] = Array.fill(len)(byte)

  testRepartition(
    "input byte array is uploadable",
    Array(baf(5, 0), baf(5, 1)),
    Array(baf(5, 0), baf(5, 1)),
    4
  )

  testRepartition("repartition down to single", Array(ba(0), ba(1), ba(3)), Array(ba(0, 1, 3)), 10)

  testRepartition(
    "some partitions too small",
    Array(ba(0), ba(1, 2, 3), ba(4, 5, 6)),
    Array(ba(0, 1, 2, 3), ba(4, 5, 6)),
    2
  )

  testRepartition(
    "input partition same size as desired partition",
    Array(ba(0, 1, 2, 4), ba(5)),
    Array(ba(0, 1, 2, 4), ba(5)),
    2
  )

  testRepartition(
    "complex example",
    Array(ba(0, 0, 0, 0, 1, 1), ba(1, 1), ba(2, 2, 2), ba(2, 3, 3, 3, 3), ba(4, 4, 4, 4), ba(5)),
    (0 to 4).map(v => baf(4, v.toByte)).toArray ++ Array(ba(5)),
    2
  )
}
