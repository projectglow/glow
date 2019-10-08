package org.projectglow.core.sql

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

class SingleFileWriterSuite extends HLSBaseTest {
  test("uses service loader") {
    val outDir = Files.createTempDirectory("writer")
    assert(DummyFileUploader.counter == 0)
    SingleFileWriter.write(sparkContext.emptyRDD[Array[Byte]], outDir.resolve("monkey").toString)
    assert(DummyFileUploader.counter == 1)
    SingleFileWriter.write(sparkContext.emptyRDD[Array[Byte]], outDir.resolve("orangutan").toString)
    assert(DummyFileUploader.counter == 1)
  }
}

class DummyFileUploader extends BigFileUploader {
  override def canUpload(conf: Configuration, path: String): Boolean = {
    path.contains("monkey")
  }

  override def upload(bytes: RDD[Array[Byte]], path: String): Unit = {
    DummyFileUploader.counter += 1
  }
}

object DummyFileUploader {
  var counter = 0
}
