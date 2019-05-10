package com.databricks.vcf

import java.io.StringReader

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import htsjdk.samtools.ValidationStringency
import htsjdk.tribble.readers.{LineIteratorImpl, SynchronousLineReader}
import htsjdk.variant.variantcontext.writer.{Options, VariantContextWriterBuilder}
import htsjdk.variant.variantcontext.{
  GenotypeBuilder,
  VariantContextBuilder,
  VariantContext => HtsjdkVariantContext
}
import htsjdk.variant.vcf._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types.StructType
import org.bdgenomics.adam.converters.{DefaultHeaderLines, VariantContextConverter}
import org.bdgenomics.adam.sql.VariantContext

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
      parseHeaderFromString(header.get)
    } else if (extraLines.isDefined) {
      parseHeaderFromString(extraLines.get) ++ defaultLines
    } else {
      defaultLines
    }
  }

  private def parseHeaderFromString(s: String): Seq[VCFHeaderLine] = {
    val stringReader = new StringReader(s)
    val lineReader = new SynchronousLineReader(stringReader)
    val lineIterator = new LineIteratorImpl(lineReader)
    val codec = new VCFCodec()
    try {
      val header = codec.readActualHeader(lineIterator).asInstanceOf[VCFHeader]
      header.getMetaDataInInputOrder.asScala.toSeq
    } catch {
      case NonFatal(e) =>
        logger.warn(s"Wasn't able to parse VCF header in $s. $e")
        Nil
    }
  }

  def getValidationStringency(options: Map[String, String]): ValidationStringency = {
    val stringency = options.getOrElse("validationStringency", "SILENT").toUpperCase
    ValidationStringency.valueOf(stringency)
  }
}

class VCFFileWriter(
    path: String,
    context: TaskAttemptContext,
    rowConverter: InternalRowToHtsjdkConverter)
    extends OutputWriter
    with HLSLogging {

  val outputStream = CodecStreams.createOutputStream(context, new Path(path))
  val writerBuilder = new VariantContextWriterBuilder().clearOptions
    .setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
    .setOutputStream(outputStream)
  var writer = writerBuilder.build
  var usedDefaultSampleNames = false
  var headerHasBeenWritten = false

  // Write header with the samples from the first variant context
  // If any sample names are missing, replace them with a default.
  private def maybeWriteHeaderWithSampleNames(vc: HtsjdkVariantContext): Unit = {
    if (!headerHasBeenWritten) {
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
        new VCFHeader(rowConverter.getHeaderLines.toSet.asJava, newSampleNames.asJava)
      } else {
        new VCFHeader(rowConverter.getHeaderLines.toSet.asJava, vc.getSampleNamesOrderedByName)
      }
      writer.writeHeader(header)
      headerHasBeenWritten = true
    }
  }

  // Header must be written before closing writer, or else VCF readers will break.
  private def maybeWriteHeaderForEmptyFile(): Unit = {
    if (!headerHasBeenWritten) {
      val header = new VCFHeader(rowConverter.getHeaderLines.toSet.asJava)
      writer.writeHeader(header)
      headerHasBeenWritten = true
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

case class VCFRowToHtsjdkConverter(
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

case class AdamToHtsjdkConverter(
    headerLines: Seq[VCFHeaderLine],
    validationStringency: ValidationStringency)
    extends InternalRowToHtsjdkConverter {

  private val converter = new VariantContextConverter(
    getHeaderLines,
    validationStringency,
    setNestedAnnotationInGenotype = true
  )
  private val encoder = Encoders
    .product[VariantContext]
    .asInstanceOf[ExpressionEncoder[VariantContext]]
    .resolveAndBind()

  override def getHeaderLines: Seq[VCFHeaderLine] = {
    headerLines
  }

  override def convert(row: InternalRow): Option[HtsjdkVariantContext] = {
    val encodedRow = encoder.fromRow(row)
    val adamVc = encodedRow.toModel()
    converter.convert(adamVc)
  }
}
