package com.databricks.vcf

import java.io.{OutputStream, StringReader}
import java.util.{List => JList, Set => JSet}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.writer.{Options, VariantContextWriterBuilder}
import htsjdk.variant.variantcontext.{GenotypeBuilder, VariantContextBuilder, VariantContext => HtsjdkVariantContext}
import htsjdk.variant.vcf._
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType

import com.databricks.hls.common.HLSLogging

object VCFFileWriter extends HLSLogging {

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

  def getValidationStringency(options: Map[String, String]): ValidationStringency = {
    val stringency = options.getOrElse("validationStringency", "SILENT").toUpperCase
    ValidationStringency.valueOf(stringency)
  }
}

class VCFFileWriter(
    outputStream: OutputStream,
    rowConverter: InternalRowToHtsjdkConverter,
    writeHeader: Boolean)
    extends OutputWriter
    with HLSLogging {

  private val writer = {
    new VariantContextWriterBuilder()
      .clearOptions
      .setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
      .setOutputStream(outputStream)
      .build
  }

  private var usedDefaultSampleNames = false
  private var headerHasBeenSet = false

  private def shouldWriteHeader: Boolean = writeHeader && !headerHasBeenSet

  // Write header with sample IDs. This can be provided via the header option or inferred from
  // the first variant context. If any sample names are missing, replaces them with a default.
  private def maybeWriteHeaderWithSampleNames(vc: HtsjdkVariantContext): Unit = {
    if (!headerHasBeenSet) {
      val header = if (rowConverter.hasSampleList) {
        rowConverter.getHeader
      } else if (vc.getSampleNamesOrderedByName.asScala.exists(_.isEmpty)) {
        val sampleNames = if (vc.hasGenotypes) {
          logger.warn(
            "Missing sample names. Assuming the sample genotypes are consistently " +
            "ordered and using default sample names."
          )
        }
        var missingSampleIdx = 0
        val newSampleNames = vc.getSampleNamesOrderedByName.asScala.map { s =>
          if (s.isEmpty) {
            missingSampleIdx += 1
            s"sample_$missingSampleIdx"
          } else {
            s
          }
        }
        usedDefaultSampleNames = true
        new VCFHeader(rowConverter.getHeader.getMetaDataInInputOrder, newSampleNames.asJava)
      } else {
        new VCFHeader(
          rowConverter.getHeader.getMetaDataInInputOrder,
          vc.getSampleNamesOrderedByName
        )
      }

      if (shouldWriteHeader) {
        writer.writeHeader(header)
      } else {
        writer.setHeader(header)
      }

      headerHasBeenSet = true
    }
  }

  // Header must be written before closing writer, or else VCF readers will break.
  private def maybeWriteHeaderForEmptyFile(): Unit = {
    if (shouldWriteHeader) {
      logger.info(s"Writing header for empty file")
      writer.writeHeader(rowConverter.getHeader)
      headerHasBeenSet = true
    }
  }

  private def maybeReplaceMissingSampleNames(vc: HtsjdkVariantContext): HtsjdkVariantContext = {
    if (usedDefaultSampleNames) {
      var missingSampleIdx = 0
      val gtSeq = vc.getGenotypes.asScala.map { gt =>
        if (gt.getSampleName.isEmpty) {
          missingSampleIdx += 1
          val gtBuilder = new GenotypeBuilder(gt)
          gtBuilder.name(s"sample_$missingSampleIdx").make
        } else {
          gt
        }
      }
      val vcBuilder = new VariantContextBuilder(vc)
      vcBuilder.genotypes(gtSeq.asJava).make
    } else {
      vc
    }
  }

  override def write(row: InternalRow): Unit = {
    rowConverter.convert(row).foreach { vc =>
      maybeWriteHeaderWithSampleNames(vc)
      writer.add(maybeReplaceMissingSampleNames(vc))
    }
  }

  override def close(): Unit = {
    maybeWriteHeaderForEmptyFile()
    writer.close()
  }
}

/**
 * Converter from an internal row to an HTSJDK VariantContext.
 * Also contains information about the header used to construct the converter. In particular,
 * it specifies the header lines and whether the samples were provided. Note that having no samples
 * is different from missing sample information, although the header used to construct the converter
 * does not distinguish between these cases.
 */
abstract class InternalRowToHtsjdkConverter {
  def getHeader: VCFHeader
  def hasSampleList: Boolean
  def convert(row: InternalRow): Option[HtsjdkVariantContext]
}

case class DefaultInternalRowToHtsjdkConverter(
    dataSchema: StructType,
    headerLineSet: JSet[VCFHeaderLine],
    providedSampleList: Option[JList[String]],
    validationStringency: ValidationStringency)
    extends InternalRowToHtsjdkConverter {

  val header: VCFHeader = providedSampleList
    .map(new VCFHeader(headerLineSet, _))
    .getOrElse(new VCFHeader(headerLineSet))

  val converter = new InternalRowToVariantContextConverter(
    dataSchema,
    header,
    validationStringency
  )
  converter.validate()

  override def getHeader: VCFHeader = {
    header
  }

  override def hasSampleList: Boolean = {
    providedSampleList.isDefined
  }

  override def convert(row: InternalRow): Option[HtsjdkVariantContext] = {
    converter.convert(row)
  }
}
