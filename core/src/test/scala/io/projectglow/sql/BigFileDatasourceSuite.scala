/*
 * Copyright 2019 The Glow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.projectglow.sql

import java.nio.file.{Files, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}

class BigFileDatasourceSuite extends GlowBaseTest {
  test("save mode: append") {
    val outFile = Files.createTempFile("tmp", ".tmp").toString
    val e = intercept[RuntimeException] {
      spark
        .emptyDataFrame
        .write
        .mode(SaveMode.Append)
        .format("io.projectglow.sql.DummyBigFileDatasource")
        .save(outFile)
    }
    assert(
      e.getMessage
        .contains("Append mode is not supported by io.projectglow.sql.DummyBigFileDatasource"))
  }

  test("save mode: overwrite") {
    val outDir = Files.createTempDirectory("tmp").toString
    spark
      .emptyDataFrame
      .write
      .mode(SaveMode.Overwrite)
      .format("io.projectglow.sql.DummyBigFileDatasource")
      .save(outDir)

    val filePath = Paths.get(outDir)
    assert(Files.isRegularFile(filePath))
    val writtenBytes = Files.readAllBytes(filePath)
    assert(writtenBytes.toSeq == Seq(0, 1, 2).map(_.toByte))
  }

  test("save mode: error if exists") {
    val outFile = Files.createTempFile("tmp", ".tmp").toString
    val e = intercept[RuntimeException] {
      spark
        .emptyDataFrame
        .write
        .mode(SaveMode.ErrorIfExists)
        .format("io.projectglow.sql.DummyBigFileDatasource")
        .save(outFile)
    }
    assert(e.getMessage.contains(s"Path $outFile already exists"))
  }

  test("save mode: ignore") {
    val outDir = Files.createTempDirectory("tmp").toString
    spark
      .emptyDataFrame
      .write
      .mode(SaveMode.Ignore)
      .format("io.projectglow.sql.DummyBigFileDatasource")
      .save(outDir)

    val dirPath = Paths.get(outDir)
    assert(Files.isDirectory(dirPath))
  }

  test("new hadoop conf picks up spark confs and options") {
    val outFile = Files.createTempDirectory("tmp").resolve("tmp").toString
    val sess = spark
    sess.conf.set("hello", "world")
    sess
      .emptyDataFrame
      .write
      .option("foo", "bar")
      .format("io.projectglow.sql.DummyBigFileDatasource")
      .save(outFile)

    assert(BigFileDatasourceSuiteFileUploader.hello == "world")
  }
}

class DummyBigFileDatasource extends BigFileDatasource {
  override def serializeDataFrame(
      options: Map[String, String],
      data: DataFrame): RDD[Array[Byte]] = {
    data.sqlContext.sparkContext.parallelize(Seq(Array(0, 1, 2).map(_.toByte)))
  }
}

class BigFileDatasourceSuiteFileUploader extends BigFileUploader {
  override def canUpload(path: String, conf: Configuration): Boolean = {
    conf.get("foo") == "bar"
  }

  override def upload(bytes: RDD[Array[Byte]], path: String, conf: Configuration): Unit = {
    BigFileDatasourceSuiteFileUploader.hello = conf.get("hello")
  }
}

object BigFileDatasourceSuiteFileUploader {
  var hello = ""
}
