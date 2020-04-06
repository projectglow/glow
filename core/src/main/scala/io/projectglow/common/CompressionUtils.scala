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

package io.projectglow.common

import java.io.BufferedInputStream

import scala.util.control.NonFatal

import com.google.common.util.concurrent.Striped
import htsjdk.samtools.util.BlockCompressedInputStream
import io.projectglow.sql.util.BGZFCodec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.storage.StorageLevel

object CompressionUtils {

  /**
   * Adds BGZF support to an existing Hadoop conf.
   */
  def hadoopConfWithBGZ(conf: Configuration): Configuration = {
    val toReturn = new Configuration(conf)
    val bgzCodecs = Seq(
      classOf[BGZFCodec].getCanonicalName,
      classOf[BGZFEnhancedGzipCodec].getCanonicalName
    )
    val codecs = toReturn
        .get("io.compression.codecs", "")
        .split(",")
        .filter(codec => codec.nonEmpty && !bgzCodecs.contains(codec)) ++ bgzCodecs
    toReturn.set("io.compression.codecs", codecs.mkString(","))
    toReturn
  }

  /** Checks whether the file is a valid bgzipped file
   * Used by filteredVariantBlockRange to abandon use of tabix if the file is not bgzipped.
   */
  def isValidBGZ(path: Path, conf: Configuration): Boolean = {
    val fs = path.getFileSystem(conf)
    WithUtils.withCloseable(fs.open(path)) { is =>
      val buffered = new BufferedInputStream(is)
      BlockCompressedInputStream.isValidFile(buffered)
    }
  }

  /**
   * Checks whether a file is gzipped (not block gzipped).
   */
  def isGzip(split: PartitionedFile, conf: Configuration): Boolean = {
    val path = new Path(split.filePath)
    val compressionCodec = new CompressionCodecFactory(hadoopConfWithBGZ(conf)).getCodec(path)
    if (compressionCodec.isInstanceOf[BGZFEnhancedGzipCodec]) {
      !isValidBGZ(path, conf)
    } else {
      false
    }
  }
}
