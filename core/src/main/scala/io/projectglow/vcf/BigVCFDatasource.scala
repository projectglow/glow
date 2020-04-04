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
import org.apache.spark.sql.{DataFrame, SQLUtils}
import org.apache.spark.sql.sources.DataSourceRegister
import org.seqdoop.hadoop_bam.util.DatabricksBGZFOutputStream

import io.projectglow.common.logging.{HlsEventRecorder, HlsTagValues}
import io.projectglow.sql.BigFileDatasource
import io.projectglow.sql.util.{ComDatabricksDataSource, SerializableConfiguration}

class BigVCFDatasource extends BigFileDatasource with DataSourceRegister {

  override def shortName(): String = "bigvcf"

  override def serializeDataFrame(
      options: Map[String, String],
      data: DataFrame): RDD[Array[Byte]] = {
    BigVCFDatasource.serializeDataFrame(options, data)
  }
}

class ComDatabricksBigVCFDatasource extends BigVCFDatasource with ComDatabricksDataSource

object BigVCFDatasource extends HlsEventRecorder {

  def serializeDataFrame(options: Map[String, String], data: DataFrame): RDD[Array[Byte]] = {

    recordHlsEvent(HlsTagValues.EVENT_BIGVCF_WRITE)
    VCFFileFormat.requireWritableAsVCF(data.schema)

    val schema = data.schema
    val rawRdd = data.queryExecution.toRdd

    val inputRdd = if (rawRdd.getNumPartitions == 0) {
      logger.warn("Writing VCF header only as the input DataFrame has zero partitions.")
      SQLUtils.createEmptyRDD(data.sparkSession)
    } else {
      rawRdd
    }
    val nParts = inputRdd.getNumPartitions

    val conf = VCFFileFormat.hadoopConfWithBGZ(data.sparkSession.sparkContext.hadoopConfiguration)
    val serializableConf = new SerializableConfiguration(conf)
    val firstNonemptyPartition =
      inputRdd.mapPartitions(iter => Iterator(iter.nonEmpty)).collect.indexOf(true)

    if (firstNonemptyPartition == -1 && options
        .get(VCFHeaderUtils.VCF_HEADER_KEY)
        .forall(_ == VCFHeaderUtils.INFER_HEADER)) {
      throw new SparkException("Cannot infer header for empty VCF.")
    }

    val (headerLineSet, providedSampleIds) = VCFHeaderUtils.parseHeaderLinesAndSamples(
      options,
      Some(VCFHeaderUtils.INFER_HEADER),
      schema,
      conf)
    val sampleIdInfo = if (providedSampleIds.isInstanceOf[SampleIds]) {
      providedSampleIds
    } else {
      VCFWriterUtils.inferSampleIdsIfPresent(data)
    }
    val validationStringency = VCFOptionParser.getValidationStringency(options)

    inputRdd.mapPartitionsWithIndex {
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
        val partitionWithHeader = if (firstNonemptyPartition == -1) 0 else firstNonemptyPartition

        val writer =
          new VCFFileWriter(
            headerLineSet,
            sampleIdInfo,
            validationStringency,
            schema,
            conf,
            outputStream,
            idx == partitionWithHeader)

        it.foreach { row =>
          writer.write(row)
        }

        writer.close()
        Iterator(baos.toByteArray)
    }
  }
}
