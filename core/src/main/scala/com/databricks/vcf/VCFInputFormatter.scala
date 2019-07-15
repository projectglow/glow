package com.databricks.vcf

import java.io.{InputStream, OutputStream, PrintWriter}
import java.net.{URI, URISyntaxException}

import scala.collection.JavaConverters._

import htsjdk.samtools.ValidationStringency
import htsjdk.tribble.readers.{AsciiLineReader, AsciiLineReaderIterator}
import htsjdk.variant.variantcontext.writer.{Options, VariantContextWriter, VariantContextWriterBuilder}
import htsjdk.variant.vcf._
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.bdgenomics.adam.rdd.VCFMetadataLoader

import com.databricks.hls.common.HLSLogging
import com.databricks.hls.transformers.{InputFormatter, OutputFormatter}

/**
 * An input formatter that writes rows as VCF records.
 * @param baseHeader A VCF header that describes the output schema. This header does not necessarily
 *                   need to include sample names. The names will be pulled from the first
 *                   input row if not present in the header.
 * @param schema The schema of the input rows.
 */
class VCFInputFormatter(baseHeader: VCFHeader, schema: StructType)
    extends InputFormatter
    with HLSLogging {

  private val converter =
    new InternalRowToVariantContextConverter(schema, baseHeader, ValidationStringency.SILENT)
  private var writer: VariantContextWriter = _
  private var stream: OutputStream = _

  override def init(stream: OutputStream): Unit = {
    this.stream = stream
  }

  override def write(record: InternalRow): Unit = {
    val vcOpt = converter.convert(record)
    if (vcOpt.isDefined) {
      val vc = vcOpt.get
      if (writer == null) {
        val sampleNames = baseHeader.getGenotypeSamples.asScala
        val realHeader = if (sampleNames.isEmpty || sampleNames.exists(_.isEmpty)) {
          new VCFHeader(baseHeader.getMetaDataInInputOrder, vc.getSampleNames)
        } else {
          baseHeader
        }
        writer = new VariantContextWriterBuilder()
          .clearOptions()
          .setOutputStream(stream)
          .setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
          .build
        writer.writeHeader(realHeader)
      }
      writer.add(vc)
    }
  }

  override def writeDummyDataset(): Unit = {
    val sampleNames = baseHeader.getGenotypeSamples.asScala
    val nSamples = baseHeader.getNGenotypeSamples
    val realHeader = if (sampleNames.isEmpty || sampleNames.exists(_.isEmpty)) {
      val fakeSampleNames = (1 to nSamples).map(n => s"sample_$n")
      new VCFHeader(baseHeader.getMetaDataInInputOrder, fakeSampleNames.asJava)
    } else {
      baseHeader
    }

    // Sample VCF record with all info and format fields missing
    val dummyVcfLine = (Seq("21", "10002403", ".", "G", "A", "19.81") ++
    Array.fill(nSamples)(".")).mkString("\t")
    val writer = new VariantContextWriterBuilder()
      .clearOptions()
      .setOutputStream(stream)
      .setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
      .build
    writer.writeHeader(realHeader)
    // checkError flushes the writer without closing the underlying stream
    writer.checkError()

    val printWriter = new PrintWriter(stream)
    printWriter.println(dummyVcfLine) // scalastyle:ignore
  }

  override def close(): Unit = {
    logger.info(s"Closing VCF input formatter")
    IOUtils.closeQuietly(writer)
  }
}

object VCFInputFormatter extends HLSLogging {
  private val VCF_HEADER_KEY = "vcfHeader"
  private val INFER_HEADER = "infer"
  private val DEFAULT_HEADER = "default"
  private def isCustomHeader(content: String): Boolean = {
    content.trim().startsWith("#")
  }

  def parseHeader(options: Map[String, String], df: DataFrame): VCFHeader = {
    require(options.contains(VCF_HEADER_KEY), "Must specify a method to determine VCF header")
    options(VCF_HEADER_KEY) match {
      case INFER_HEADER =>
        logger.info("Inferring header for VCF writer")
        new VCFHeader(VCFSchemaInferer.headerLinesFromSchema(df.schema).toSet.asJava)
      case DEFAULT_HEADER =>
        logger.info(s"Using default header lines for VCF writer")
        new VCFHeader(VCFRowHeaderLines.allHeaderLines.toSet.asJava)
      case content if isCustomHeader(content) =>
        logger.info(s"Using provided string as VCF header")
        VCFFileWriter.parseHeaderFromString(content)
      case path => // Input is a path
        logger.info(s"Attempting to parse VCF header from path $path")
        try {
          // Verify that string is a valid URI
          new URI(path)
          VCFMetadataLoader.readVcfHeader(df.sparkSession.sparkContext.hadoopConfiguration, path)
        } catch {
          case _: URISyntaxException =>
            throw new IllegalArgumentException(s"Could not parse VCF header from path $path")
        }
    }
  }
}

class VCFOutputFormatter extends OutputFormatter with HLSLogging {
  override def makeIterator(schema: StructType, stream: InputStream): Iterator[InternalRow] = {
    val codec = new VCFCodec
    val lineIterator = new AsciiLineReaderIterator(AsciiLineReader.from(stream))
    val header = codec.readActualHeader(lineIterator).asInstanceOf[VCFHeader]
    val schemaFromHeader = VCFSchemaInferer.inferSchema(true, true, header)
    require(
      schemaFromHeader == schema,
      s"Data schema did not match provided schema. " +
      s"Data schema: $schemaFromHeader Provided schema: $schema")
    val converter =
      new VariantContextToInternalRowConverter(header, schema, ValidationStringency.LENIENT)

    new Iterator[InternalRow] {
      private var nextRecord: InternalRow = null
      private def readNextVc(): Unit = {
        while (nextRecord == null && lineIterator.hasNext) {
          val decoded = codec.decode(lineIterator.next())
          if (decoded != null) {
            nextRecord = converter.convertRow(decoded, isSplit = false)
          }
        }
      }

      override def hasNext: Boolean = {
        readNextVc()
        nextRecord != null
      }

      override def next(): InternalRow = {
        readNextVc()
        if (nextRecord != null) {
          val ret = nextRecord
          nextRecord = null
          ret
        } else {
          throw new NoSuchElementException("Iterator is empty")
        }
      }
    }
  }

  override def outputSchema(stream: InputStream): StructType = {
    val codec = new VCFCodec
    val lineIterator = new AsciiLineReaderIterator(AsciiLineReader.from(stream))
    val header = codec.readActualHeader(lineIterator).asInstanceOf[VCFHeader]
    VCFSchemaInferer.inferSchema(true, true, header)
  }
}
