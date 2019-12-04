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
import org.apache.spark.sql.types.{ArrayType, StructType}
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

    val schema = data.schema
    val rdd = data.queryExecution.toRdd

    val nParts = rdd.getNumPartitions

    if (nParts == 0) {
      throw new SparkException(
        "Cannot write vcf because the DataFrame has zero partitions. " +
        "Repartition to a positive number of partitions if you want to just write the header")
    }

    val conf = VCFFileFormat.hadoopConfWithBGZ(data.sparkSession.sparkContext.hadoopConfiguration)
    val serializableConf = new SerializableConfiguration(conf)
    val firstNonemptyPartition =
      rdd.mapPartitions(iter => Iterator(iter.nonEmpty)).collect.indexOf(true)

    if (firstNonemptyPartition == -1 && (!options.contains(VCFHeaderUtils.VCF_HEADER_KEY) ||
      options
        .get(VCFHeaderUtils.VCF_HEADER_KEY)
        .contains(VCFHeaderUtils.INFER_HEADER))) {
      throw new SparkException("Cannot infer header for empty VCF.")
    }

    val (headerLineSet, providedSampleIdsOpt) = VCFHeaderUtils.parseHeaderLinesAndSamples(
      options,
      Some(VCFHeaderUtils.INFER_HEADER),
      schema,
      conf)
    val maybeInferredSampleIds = providedSampleIdsOpt.getOrElse(inferSampleIds(data))
    val validationStringency = VCFOptionParser.getValidationStringency(options)

    rdd.mapPartitionsWithIndex {
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
            Some(maybeInferredSampleIds),
            validationStringency,
            schema,
            conf,
            outputStream,
            idx == partitionWithHeader)

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

  /**
   * Infer sample IDs from a genomic DataFrame.
   *
   * - If there are no genotypes, there are no sample IDs.
   * - If there are genotypes and sample IDs are all...
   *     - Missing, the sample IDs are inferred from the number of genotypes per row (must be the same per row).
   *     - Present, the sample IDs are found by unifying non-null sample IDs across all rows.
   *
   * Missing sample IDs are represented by an empty String, as that is the default VariantContext sample ID. They will
   * be replaced with defaults by [[VCFStreamWriter.replaceEmptySampleIds]].
   */
  def inferSampleIds(data: DataFrame): Seq[String] = {
    val genotypeSchemaOpt = data
      .schema
      .find(_.name == "genotypes")
      .map(_.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])
    if (genotypeSchemaOpt.isEmpty) {
      return Seq.empty
    }

    import data.sparkSession.implicits._
    val hasSampleIds = genotypeSchemaOpt.get.exists(_.name == "sampleId")
    if (hasSampleIds) {
      val distinctSampleLists = data
        .select("genotypes.sampleId")
        .distinct()
        .as[Array[String]]
        .collect

      val inferredSamples = distinctSampleLists
        .flatten
        .distinct
        .filter(_ != null)
        .sorted

      inferredSamples ++ getMissingSamples(numMissingSampleList)
    } else {
      val numMissingSampleList = data
        .selectExpr("size(genotypes)")
        .distinct()
        .as[Int]
        .collect

      getMissingSamples(numMissingSampleList)
    }
  }

  def getMissingSamples(numMissingSamplesList: Seq[Int]): Seq[String] = {
    if (numMissingSamplesList.length > 1) {
      throw new IllegalArgumentException(
        "Rows contain varying number of missing samples; cannot infer VCF header.")
    }
    Array.fill(numMissingSamplesList.head)("")
  }
}
