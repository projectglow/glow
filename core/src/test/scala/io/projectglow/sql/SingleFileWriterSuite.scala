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

import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

class SingleFileWriterSuite extends GlowBaseTest {
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
