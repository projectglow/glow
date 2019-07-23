package com.databricks.hls.transformers

import scala.collection.JavaConverters._

import java.io.InputStream

import com.univocity.parsers.csv.CsvParser
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.csv.{CSVDataSource, CSVOptions, CSVUtils, DummyCsvDataSource, UnivocityParser, UnivocityParserUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class CSVOutputFormatter(parsedOptions: CSVOptions) extends OutputFormatter {

  // Does not automatically infer schema types
  override def outputSchema(stream: InputStream): StructType = {
    val tokenizedRows = UnivocityParserUtils.tokenizeStream(
      stream,
      shouldDropHeader = false,
      new CsvParser(parsedOptions.asParserSettings))
    if (!tokenizedRows.hasNext) {
      throw new IllegalStateException("CSV needs header to infer schema.")
    }
    val caseSensitive = SQLConf.get.caseSensitiveAnalysis
    val header =
      DummyCsvDataSource.makeSafeHeaderShim(tokenizedRows.next, caseSensitive, parsedOptions)
    val fields = header.map { fieldName =>
      StructField(fieldName, StringType, nullable = true)
    }
    StructType(fields)
  }

  override def makeIterator(schema: StructType, stream: InputStream): Iterator[InternalRow] = {
    val univocityParser = new UnivocityParser(schema, schema, parsedOptions)
    val lines = IOUtils.lineIterator(stream, "UTF-8").asScala

    if (univocityParser.options.headerFlag) {
      CSVUtils.extractHeader(lines, univocityParser.options).foreach { header =>
        val columnNames = univocityParser.tokenizer.parseLine(header)
        CSVDataSource.checkHeaderColumnNames(
          schema,
          columnNames,
          "NO_FILE",
          univocityParser.options.enforceSchema,
          SQLConf.get.caseSensitiveAnalysis)
      }
    }
    UnivocityParserUtils.parseIterator(lines, univocityParser, schema)
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
