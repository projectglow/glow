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

package io.projectglow.gff

import io.projectglow.common.logging.HlsEventRecorder
import io.projectglow.common.{CompressionUtils, FeatureSchemas, GlowLogging}
import io.projectglow.sql.util.SerializableConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.csv.{CSVDataSource, CSVFileFormat, CSVOptions, UnivocityParser}
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.datasources.{DataSource, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object GffDataSource {

 def createBaseDataset(
    sparkSession: SparkSession,
    inputPaths: Seq[FileStatus],
    options: CSVOptions): Dataset[String] = {
    val paths = inputPaths.map(_.getPath.toString)
      sparkSession.baseRelationToDataFrame(
        DataSource.apply(
          sparkSession,
          paths = paths,
          className = classOf[TextFileFormat].getName,
          options = options.parameters
        ).resolveRelation(checkFilesExist = false))
        .select("value").as[String](Encoders.STRING)
  }

}

