package com.databricks.vcf

import scala.collection.JavaConverters._
import java.io.OutputStream

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
