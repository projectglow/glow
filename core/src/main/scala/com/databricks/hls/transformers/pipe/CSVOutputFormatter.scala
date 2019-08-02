package com.databricks.hls.transformers.pipe

import java.io.{BufferedInputStream, IOException}

import scala.collection.JavaConverters._

import com.univocity.parsers.csv.CsvParser
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.csv.{CSVDataSourceUtils, CSVOptions, UnivocityParser, UnivocityParserUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

class CSVOutputFormatter(parsedOptions: CSVOptions) extends OutputFormatter {

  private def getFirstRecord(stream: BufferedInputStream): Option[Array[String]] = {
    val tokenizer = new CsvParser(parsedOptions.asParserSettings)
    tokenizer.beginParsing(stream)
    Option(tokenizer.parseNext())
  }

  private def getSchema(record: Array[String]): StructType = {
    val header =
      CSVDataSourceUtils.makeSafeHeader(record, SQLConf.get.caseSensitiveAnalysis, parsedOptions)
    val fields = header.map { fieldName =>
      StructField(fieldName, StringType, nullable = true)
    }
    StructType(fields)
  }

  /**
   * Reads the first record to infer the schema, then jumps back to the beginning of the stream to
   * parse the entire stream.
   */
  override def makeIterator(stream: BufferedInputStream): Iterator[Any] = {
    // Mark the beginning of the stream to return here after schema inference.
    // Up to 1KB can be read before this mark becomes invalid; in this case the bytes between the
    // marked position and the current position may be lost from the buffered array.
    stream.mark(1000)

    val firstRecordOpt = getFirstRecord(stream)
    if (firstRecordOpt.isEmpty) {
      return Iterator.empty
    }

    val firstRecord = firstRecordOpt.get
    val schema = getSchema(firstRecord)
    val univocityParser = new UnivocityParser(schema, schema, parsedOptions)

    val parsedIter = try {
      // Don't throw away the first record; return to the marked beginning of the stream.
      stream.reset()
      val lines = IOUtils.lineIterator(stream, "UTF-8").asScala.toSeq
      UnivocityParserUtils.parseIterator(lines.toIterator, univocityParser, schema)
    } catch {
      case _: IOException =>
        // Stream may have been closed by the CsvParser if there is only one row
        Iterator(
          new GenericInternalRow(firstRecord.map(UTF8String.fromString).asInstanceOf[Array[Any]]))
    }
    val parsedIterWithoutHeader = if (parsedOptions.headerFlag) {
      parsedIter.drop(1)
    } else {
      parsedIter
    }
    Iterator(schema) ++ parsedIterWithoutHeader
  }
}

class CSVOutputFormatterFactory extends OutputFormatterFactory {
  override def name: String = "csv"

  override def makeOutputFormatter(options: Map[String, String]): OutputFormatter = {
    val parsedOptions =
      new CSVOptions(options, SQLConf.get.csvColumnPruning, SQLConf.get.sessionLocalTimeZone)
    new CSVOutputFormatter(parsedOptions)
  }
}
