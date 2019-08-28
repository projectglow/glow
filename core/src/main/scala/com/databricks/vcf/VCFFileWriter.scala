package com.databricks.vcf

import java.io.{OutputStream, StringReader}
import java.net.{URI, URISyntaxException}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import htsjdk.variant.vcf._
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import com.google.common.annotations.VisibleForTesting
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructType
import org.bdgenomics.adam.rdd.VCFMetadataLoader

import com.databricks.hls.common.HLSLogging

object VCFFileWriter extends HLSLogging {

  val VCF_HEADER_KEY = "vcfHeader"
  private val INFER_HEADER = "infer"

  def parseHeaderFromString(s: String): VCFHeader = {
    val stringReader = new StringReader(s)
    val lineIterator = new LineIteratorImpl(IOUtils.lineIterator(stringReader).asScala)
    val codec = new VCFCodec()
    try {
      codec.readActualHeader(lineIterator).asInstanceOf[VCFHeader]
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Wasn't able to parse VCF header in $s. $e")
        new VCFHeader()
    }
  }

  private def isCustomHeader(content: String): Boolean = {
    content.trim().startsWith("#")
  }

  /**
   * Returns VCF header lines and sample IDs (if provided) based on the VCF header option.
   * If inferring header lines from the schema or using default header lines, no sample IDs can
   * be returned.
   * If reading a VCF header from a string or a file, the sample IDs are returned.
   */
  @VisibleForTesting
  private[databricks] def parseHeaderLinesAndSamples(
      options: Map[String, String],
      defaultHeader: Option[String],
      schema: StructType,
      conf: Configuration): (Set[VCFHeaderLine], Option[Seq[String]]) = {

    require(
      options.contains(VCF_HEADER_KEY) || defaultHeader.isDefined,
      "Must specify a method to determine VCF header")
    options.getOrElse(VCF_HEADER_KEY, defaultHeader.get) match {
      case INFER_HEADER =>
        logger.info("Inferring header for VCF writer")
        (VCFSchemaInferrer.headerLinesFromSchema(schema).toSet, None)
      case content if isCustomHeader(content) =>
        logger.info("Using provided string as VCF header")
        val header = VCFFileWriter.parseHeaderFromString(content)
        (header.getMetaDataInInputOrder.asScala.toSet, Some(header.getGenotypeSamples.asScala))
      case path => // Input is a path
        logger.info(s"Attempting to parse VCF header from path $path")
        try {
          // Verify that string is a valid URI
          new URI(path)
          val header = VCFMetadataLoader
            .readVcfHeader(conf, path)
          (header.getMetaDataInInputOrder.asScala.toSet, Some(header.getGenotypeSamples.asScala))
        } catch {
          case _: URISyntaxException =>
            throw new IllegalArgumentException(s"Could not parse VCF header from path $path")
        }
    }
  }
}

class VCFFileWriter(
    options: Map[String, String],
    schema: StructType,
    conf: Configuration,
    stream: OutputStream,
    writeHeader: Boolean)
    extends OutputWriter
    with HLSLogging {

  private val DEFAULT_VCF_WRITER_HEADER = "infer"
  private val (headerLineSet, providedSampleIds) =
    VCFFileWriter.parseHeaderLinesAndSamples(options, Some(DEFAULT_VCF_WRITER_HEADER), schema, conf)
  private val converter = new InternalRowToVariantContextConverter(
    schema,
    headerLineSet,
    VCFOptionParser.getValidationStringency(options)
  )
  converter.validate()
  private val writer = new VCFStreamWriter(stream, headerLineSet, providedSampleIds, writeHeader)

  override def write(row: InternalRow): Unit = {
    converter.convert(row).foreach(writer.write)
  }

  override def close(): Unit = {
    writer.close()
  }
}
