package com.databricks.vcf

import scala.collection.JavaConverters._
import java.io.{OutputStream, PrintWriter}

import htsjdk.variant.variantcontext.writer.{Options, VariantContextWriterBuilder}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import com.databricks.hls.common.HLSLogging
import com.databricks.hls.transformers.{InputFormatter, InputFormatterFactory}

/**
 * An input formatter that writes rows as VCF records.
 */
class VCFInputFormatter(
    converter: InternalRowToVariantContextConverter,
    providedSampleIds: Option[Seq[String]])
    extends InputFormatter
    with HLSLogging {

  private var writer: VCFStreamWriter = _
  private var stream: OutputStream = _

  override def init(stream: OutputStream): Unit = {
    this.stream = stream
    this.writer = new VCFStreamWriter(
      stream,
      converter.vcfHeader.getMetaDataInInputOrder.asScala.toSet,
      providedSampleIds,
      writeHeader = true)
  }

  override def write(record: InternalRow): Unit = {
    val vcOpt = converter.convert(record)
    if (vcOpt.isDefined) {
      val vc = vcOpt.get
      writer.write(vc)
    }
  }

  override def writeDummyDataset(): Unit = {
    val header = converter.vcfHeader

    // Sample-free VCF record with all info and format fields missing
    val dummyVcfLine = Seq("21", "10002403", ".", "G", "A", "19.81").mkString("\t")
    val writer = new VariantContextWriterBuilder()
      .clearOptions()
      .setOutputStream(stream)
      .setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
      .build
    writer.writeHeader(header)
    // checkError flushes the writer without closing the underlying stream
    writer.checkError()

    val printWriter = new PrintWriter(stream)
    printWriter.println(dummyVcfLine) // scalastyle:ignore
  }

  override def close(): Unit = {
    logger.info("Closing VCF input formatter")
    writer.close()
  }
}

class VCFInputFormatterFactory extends InputFormatterFactory {
  override def name: String = "vcf"

  override def makeInputFormatter(df: DataFrame, options: Map[String, String]): InputFormatter = {
    val (headerLineSet, providedSampleIds) =
      VCFFileWriter.parseHeaderLinesAndSamples(
        options,
        None,
        df.schema,
        df.sparkSession.sparkContext.hadoopConfiguration)
    val rowConverter = new InternalRowToVariantContextConverter(
      df.schema,
      headerLineSet,
      VCFOptionParser.getValidationStringency(options)
    )
    rowConverter.validate()
    new VCFInputFormatter(rowConverter, providedSampleIds)
  }
}
