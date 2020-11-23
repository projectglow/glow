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

package io.projectglow.bgen

import com.google.common.io.LittleEndianDataInputStream
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import io.projectglow.common.{BgenOptions, CommonOptions, VariantSchemas, WithUtils}
import io.projectglow.sql.util.SerializableConfiguration

/**
 * Infers the schema of a set of BGEN files from the user-provided options and the header of each
 * file.
 *
 * - If sample IDs are defined in either a .sample file or in any of the headers,
 *   `sampleId` is added to the genotype field.
 * - If hard calls should be emitted (true by default), `calls` is added to the genotype field.
 */
object BgenSchemaInferrer {
  def inferSchema(
      spark: SparkSession,
      files: Seq[FileStatus],
      options: Map[String, String]): StructType = {
    val hasSampleIds = includeSampleIds(spark, files, options)
    val hasHardCalls = options.get(BgenOptions.EMIT_HARD_CALLS).forall(_.toBoolean)
    VariantSchemas.bgenDefaultSchema(hasSampleIds, hasHardCalls)
  }

  def includeSampleIds(
      spark: SparkSession,
      files: Seq[FileStatus],
      options: Map[String, String]): Boolean = {
    val shouldIncludeSampleIds = options.get(CommonOptions.INCLUDE_SAMPLE_IDS).forall(_.toBoolean)
    if (!shouldIncludeSampleIds) {
      return false
    }

    val sampleIdsFromSampleFile =
      BgenFileFormat.getSampleIds(options, spark.sparkContext.hadoopConfiguration)
    if (sampleIdsFromSampleFile.isDefined) {
      return true
    }

    val serializableConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    val ignoreExtension = options.get(BgenOptions.IGNORE_EXTENSION_KEY).exists(_.toBoolean)
    val bgenPaths =
      files.filter { fs =>
        fs.getLen > 0 && (fs
          .getPath
          .toString
          .endsWith(BgenFileFormat.BGEN_SUFFIX) || ignoreExtension)
      }.map(_.getPath.toString)
    spark
      .sparkContext
      .parallelize(bgenPaths)
      .map { path =>
        val hPath = new Path(path)
        val hadoopFs = hPath.getFileSystem(serializableConf.value)
        WithUtils.withCloseable(hadoopFs.open(hPath)) { stream =>
          val littleEndianDataInputStream = new LittleEndianDataInputStream(stream)
          new BgenHeaderReader(littleEndianDataInputStream)
            .readHeader(None)
            .sampleIds
            .exists(_.nonEmpty)
        }
      }
      .collect()
      .exists(identity)
  }
}
