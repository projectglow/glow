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
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import io.projectglow.common.{VariantSchemas, WithUtils}
import io.projectglow.sql.util.SerializableConfiguration

/**
 * Infers the schema of a set of BGEN files from the user-provided options and the header of each
 * file.
 *
 * Currently the implementation is very simple. It checks if sample IDs are defined in either a
 * .sample file or in any of the headers. If so, it returns a fixed schema that includes a
 * `sampleId` genotype field. If not, it returns the same schema without a `sampleId` field.
 */
object BgenSchemaInferrer {
  def inferSchema(
      spark: SparkSession,
      files: Seq[FileStatus],
      options: Map[String, String]): StructType = {
    val sampleIdsFromSampleFile =
      BgenFileFormat.getSampleIds(options, spark.sparkContext.hadoopConfiguration)
    if (sampleIdsFromSampleFile.isDefined) {
      return VariantSchemas.bgenDefaultSchema(hasSampleIds = true)
    }

    val serializableConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    val ignoreExtension = options.get(BgenFileFormat.IGNORE_EXTENSION_KEY).exists(_.toBoolean)
    val bgenPaths =
      files.filter { fs =>
        fs.getLen > 0 && (fs
          .getPath
          .toString
          .endsWith(BgenFileFormat.BGEN_SUFFIX) || ignoreExtension)
      }.map(_.getPath.toString)
    val hasSampleIds = spark
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

    VariantSchemas.bgenDefaultSchema(hasSampleIds)
  }
}
