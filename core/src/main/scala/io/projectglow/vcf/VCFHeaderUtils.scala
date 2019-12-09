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
import htsjdk.variant.vcf.{VCFCodec, VCFHeader, VCFHeaderLine}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType

import io.projectglow.common.GlowLogging

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
}
