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

import java.io.StringReader

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.google.common.annotations.VisibleForTesting
import htsjdk.variant.vcf.{VCFCodec, VCFCompoundHeaderLine, VCFContigHeaderLine, VCFFilterHeaderLine, VCFHeader, VCFHeaderLine}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import io.projectglow.common.GlowLogging
import io.projectglow.sql.util.SerializableConfiguration

object VCFHeaderUtils extends GlowLogging {

  val VCF_HEADER_KEY = "vcfHeader"
  val INFER_HEADER = "infer"

  def parseHeaderFromString(s: String): VCFHeader = {
    val stringReader = new StringReader(s)
    val lineIterator = new LineIteratorImpl(IOUtils.lineIterator(stringReader).asScala)
    val codec = new VCFCodec()
    try {
      codec.readActualHeader(lineIterator).asInstanceOf[VCFHeader]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Unable to parse VCF header \n$s\n$e")
    }
  }

  private def isCustomHeader(content: String): Boolean = {
    content.trim().startsWith("#")
  }

  /**
   * Returns VCF header lines and sample IDs (if provided) based on the VCF header option.
   *
   * If inferring header lines from the schema, no sample IDs can be returned.
   * If reading a VCF header from a string or a file, the sample IDs are returned.
   */
  @VisibleForTesting
  private[projectglow] def parseHeaderLinesAndSamples(
      options: Map[String, String],
      defaultHeader: Option[String],
      schema: StructType,
      conf: Configuration): (Set[VCFHeaderLine], SampleIdInfo) = {

    require(
      options.contains(VCF_HEADER_KEY) || defaultHeader.isDefined,
      "Must specify a method to determine VCF header")
    options.getOrElse(VCF_HEADER_KEY, defaultHeader.get) match {
      case INFER_HEADER =>
        logger.info("Inferring header for VCF writer")
        val headerLines = VCFSchemaInferrer.headerLinesFromSchema(schema).toSet
        (headerLines, InferSampleIds)
      case content if isCustomHeader(content) =>
        logger.info("Using provided string as VCF header")
        val header = parseHeaderFromString(content)
        (header.getMetaDataInInputOrder.asScala.toSet, SampleIds(header.getGenotypeSamples.asScala))
      case path => // Input is a path
        logger.info(s"Attempting to parse VCF header from path $path")
        // Verify that string is a valid URI
        val header = VCFMetadataLoader.readVcfHeader(conf, path)
        (header.getMetaDataInInputOrder.asScala.toSet, SampleIds(header.getGenotypeSamples.asScala))
    }
  }

  def createHeaderRDD(spark: SparkSession, files: Seq[String]): RDD[VCFHeader] = {
    val serializableConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())

    spark
      .sparkContext
      .parallelize(files)
      .flatMap { path =>
        if (path.endsWith(VCFFileFormat.INDEX_SUFFIX)) {
          None
        } else {
          val (header, _) = VCFFileFormat.createVCFCodec(path, serializableConf.value)
          Some(header)
        }
      }
  }

  /**
   * Find the unique desired header lines from an RDD of VCF headers.
   * If lines of the same class found with the same ID, we pick one unless they are incompatible.
   * If there are incompatible lines, [[IllegalArgumentException]] is thrown.
   * Incompatible lines are:
   * - FORMAT or INFO lines with the same ID but different types or counts
   * - contig lines with the same ID but different lengths
   *
   * @param headers VCF headers to parse header lines from
   * @param getNonSchemaHeaderLines If false, parses only INFO and FORMAT lines.
   *                                If true, also parses contig and filter lines.
   */
  def getUniqueHeaderLines(
      headers: RDD[VCFHeader],
      getNonSchemaHeaderLines: Boolean): Seq[VCFHeaderLine] = {
    headers.flatMap { header =>
      val schemaHeaderLines = header.getInfoHeaderLines.asScala ++ header
          .getFormatHeaderLines
          .asScala
      val nonSchemaHeaderLines = if (getNonSchemaHeaderLines) {
        header.getContigLines.asScala ++ header.getFilterLines.asScala
      } else {
        Seq.empty
      }
      schemaHeaderLines ++ nonSchemaHeaderLines
    }.keyBy(line => (line.getClass.getName, line.getID))
      .reduceByKey {
        case (line1: VCFCompoundHeaderLine, line2: VCFCompoundHeaderLine) =>
          if (line1.equalsExcludingDescription(line2)) {
            line1
          } else {
            throw new IllegalArgumentException(
              s"Found incompatible compound header lines: $line1 " +
              s"and $line2. Header lines with the same ID must have the same count and type.")
          }
        case (line1: VCFContigHeaderLine, line2: VCFContigHeaderLine) =>
          if (line1
              .getSAMSequenceRecord()
              .getSequenceLength() == line2.getSAMSequenceRecord().getSequenceLength()) {
            line1
          } else {
            throw new IllegalArgumentException(
              s"Found incompatible contig header lines: $line1 " +
              s"and $line2. Header lines with the same ID must have the same length.")
          }
        case (line1: VCFFilterHeaderLine, _: VCFFilterHeaderLine) => line1
        case (line1, _) =>
          throw new IllegalArgumentException(
            s"Collected unexpected header line type: ${line1.getClass.getName}")
      }
      .values
      .collect()
  }

  /**
   * A convenience function to parse the headers from a set of VCF files and return the desired
   * header lines.
   *
   * @param files VCF files to parse header lines from
   * @param getNonSchemaHeaderLines If false, parses only INFO and FORMAT lines.
   *                                If true, also parses contig and filter lines.
   */
  def readHeaderLines(
      spark: SparkSession,
      files: Seq[String],
      getNonSchemaHeaderLines: Boolean): Seq[VCFHeaderLine] = {
    getUniqueHeaderLines(createHeaderRDD(spark, files), getNonSchemaHeaderLines)
  }
}
