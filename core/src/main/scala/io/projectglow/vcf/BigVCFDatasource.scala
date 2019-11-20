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

package io.projectglow.vcf

import java.io.ByteArrayOutputStream

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sources.DataSourceRegister
import org.seqdoop.hadoop_bam.util.DatabricksBGZFOutputStream

import io.projectglow.common.logging.{HlsMetricDefinitions, HlsTagDefinitions, HlsTagValues, HlsUsageLogging}
import io.projectglow.sql.BigFileDatasource
import io.projectglow.sql.util.{ComDatabricksDataSource, SerializableConfiguration}

class BigVCFDatasource extends BigFileDatasource with DataSourceRegister {

  override def shortName(): String = "bigvcf"

  override def serializeDataFrame(options: Map[String, String], data: DataFrame): RDD[Array[Byte]] =
    BigVCFDatasource.serializeDataFrame(options, data)
}

class ComDatabricksBigVCFDatasource extends BigVCFDatasource with ComDatabricksDataSource

object BigVCFDatasource extends HlsUsageLogging {
  def serializeDataFrame(options: Map[String, String], data: DataFrame): RDD[Array[Byte]] = {

    val dSchema = data.schema
    val rdd = data.queryExecution.toRdd

    val nParts = rdd.getNumPartitions
    val conf = VCFFileFormat.hadoopConfWithBGZ(data.sparkSession.sparkContext.hadoopConfiguration)
    val serializableConf = new SerializableConfiguration(conf)
    val firstNonemptyPartition =
      rdd.mapPartitions(iter => Iterator(iter.nonEmpty)).collect.indexOf(true)

    if (firstNonemptyPartition == -1 && (!options.contains(VCFFileWriter.VCF_HEADER_KEY) ||
      options
        .get(VCFFileWriter.VCF_HEADER_KEY)
        .contains(VCFFileWriter.INFER_HEADER))) {
      throw new SparkException("Cannot infer header for empty VCF.")
    }

    import data.sparkSession.implicits._
    val inferredSamples =
      data
        .select("genotypes.sampleId")
        .as[Array[String]]
        .distinct()
        .collect
        .flatten
        .distinct
        .mkString("\t")

    val samplesAndByteArrays = rdd.mapPartitionsWithIndex {
      case (idx, it) =>
        val conf = serializableConf.value
        val codec = new CompressionCodecFactory(conf)
        val baos = new ByteArrayOutputStream()
        val outputStream = Option(codec.getCodec(new Path(BigFileDatasource.checkPath(options))))
          .map(_.createOutputStream(baos))
          .getOrElse(baos)

        // Write an empty GZIP block iff this is the last partition
        DatabricksBGZFOutputStream.setWriteEmptyBlockOnClose(outputStream, idx == nParts - 1)

        // Write the header if this is the first nonempty partition
        val writer =
          new VCFFileWriter(
            options + ("inferredSamples" -> inferredSamples),
            dSchema,
            conf,
            outputStream,
            idx == firstNonemptyPartition)

        it.foreach { row =>
          writer.write(row)
        }

        writer.close()

        // record bigVcfWrite event in the log
        recordHlsUsage(
          HlsMetricDefinitions.EVENT_HLS_USAGE,
          Map(
            HlsTagDefinitions.TAG_EVENT_TYPE -> HlsTagValues.EVENT_BIGVCF_WRITE
          )
        )

        Iterator(baos.toByteArray)
    }
  }
}
