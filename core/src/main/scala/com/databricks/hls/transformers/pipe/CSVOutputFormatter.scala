package com.databricks.hls.transformers.pipe

import java.io.InputStream

import scala.collection.JavaConverters._

import com.univocity.parsers.csv.CsvParser
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.execution.datasources.csv.{CSVDataSourceUtils, CSVOptions, CSVUtils, UnivocityParser, UnivocityParserUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class CSVOutputFormatter(parsedOptions: CSVOptions) extends OutputFormatter {

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
  override def makeIterator(stream: InputStream): Iterator[Any] = {
    val lines = IOUtils.lineIterator(stream, "UTF-8").asScala
    val filteredLines = CSVUtils.filterCommentAndEmpty(lines, parsedOptions)

    if (!filteredLines.hasNext) {
      return Iterator.empty
    }

    val firstLine = filteredLines.next
    val csvParser = new CsvParser(parsedOptions.asParserSettings)
    val firstRecord = csvParser.parseLine(firstLine)
    val schema = getSchema(firstRecord)
    val univocityParser = new UnivocityParser(schema, schema, parsedOptions)

    val parsedIter =
      UnivocityParserUtils.parseIterator(
        Iterator(firstLine) ++ filteredLines,
        univocityParser,
        schema)

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
