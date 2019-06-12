package com.databricks.vcf

import java.io.{OutputStream, StringReader}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import htsjdk.samtools.ValidationStringency
import htsjdk.tribble.readers.{LineIteratorImpl, SynchronousLineReader}
import htsjdk.variant.variantcontext.writer.{Options, VariantContextWriterBuilder}
import htsjdk.variant.variantcontext.{GenotypeBuilder, VariantContextBuilder, VariantContext => HtsjdkVariantContext}
import htsjdk.variant.vcf._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType

import com.databricks.hls.common.HLSLogging

object VCFFileWriter extends HLSLogging {

  /**
   * Parses header lines (written by VCFWriter) passed in as an option and, if necessary,
   * merges them with the provided default header lines.
   *
   * If the user provides an entire header with the `overrideHeaderLines` key, then we just return
   * those lines.
   *
   * If the user provides extra lines with `extraHeaderLines`, we return those lines along with the
   * defaults.
   *
   * If both options are provided, it's an error.
   *
   * If neither option is provided, we return the default header lines.
   */
  def getHeaderLines(
      defaultLines: Seq[VCFHeaderLine],
      options: Map[String, String]): Seq[VCFHeaderLine] = {
    val header = options.get("overrideHeaderLines")
    val extraLines = options.get("extraHeaderLines")
    require(
      header.isEmpty || extraLines.isEmpty,
      s"Cannot provide both override and extra " +
      s"header lines."
    )

    if (header.isDefined) {
      parseHeaderLinesFromString(header.get)
    } else if (extraLines.isDefined) {
      parseHeaderLinesFromString(extraLines.get) ++ defaultLines
    } else {
      defaultLines
    }
  }

  def parseHeaderLinesFromString(s: String): Seq[VCFHeaderLine] = {
    val header = parseHeaderFromString(s)
    header.getMetaDataInInputOrder.asScala.toSeq
  }

  def parseHeaderFromString(s: String): VCFHeader = {
    val stringReader = new StringReader(s)
    val lineReader = new SynchronousLineReader(stringReader)
    val lineIterator = new LineIteratorImpl(lineReader)
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
    new VariantContextWriterBuilder().clearOptions
      .setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
      .setOutputStream(outputStream)
      .build
  }

  private var usedDefaultSampleNames = false
  private var headerHasBeenSet = false

  private def shouldWriteHeader: Boolean = writeHeader && !headerHasBeenSet
  private def headerLines: java.util.Set[VCFHeaderLine] = {
    rowConverter.getHeaderLines.toSet.asJava
  }

  // Write header with the samples from the first variant context
  // If any sample names are missing, replace them with a default.
  private def maybeWriteHeaderWithSampleNames(vc: HtsjdkVariantContext): Unit = {
    if (!headerHasBeenSet) {
      val header = if (vc.getSampleNamesOrderedByName.asScala.exists(_.isEmpty)) {
        if (vc.hasGenotypes) {
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
        new VCFHeader(headerLines, newSampleNames.asJava)
      } else {
        new VCFHeader(headerLines, vc.getSampleNamesOrderedByName)
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
      val header = new VCFHeader(headerLines)
      writer.writeHeader(header)
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
 * Converter from an internal row (following either the VCFRow or ADAM VariantContext schema) to
 * an HTSJDK VariantContext. Includes header lines corresponding to the fields parsed out in the
 * schema for conversion purposes, as well as extra header lines passed in as options.
 */
abstract class InternalRowToHtsjdkConverter {
  def getHeaderLines: Seq[VCFHeaderLine]
  def convert(row: InternalRow): Option[HtsjdkVariantContext]
}

case class DefaultInternalRowToHtsjdkConverter(
    dataSchema: StructType,
    headerLines: Seq[VCFHeaderLine],
    validationStringency: ValidationStringency)
    extends InternalRowToHtsjdkConverter {

  val converter = new InternalRowToVariantContextConverter(
    dataSchema,
    new VCFHeader(getHeaderLines.toSet.asJava),
    validationStringency
  )
  converter.validate()

  override def getHeaderLines: Seq[VCFHeaderLine] = {
    headerLines
  }

  override def convert(row: InternalRow): Option[HtsjdkVariantContext] = {
    converter.convert(row)
  }
}
