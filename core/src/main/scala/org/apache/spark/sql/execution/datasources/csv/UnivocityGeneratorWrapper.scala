package org.apache.spark.sql.execution.datasources.csv

import java.io.{InputStream, Writer}

import com.univocity.parsers.csv.CsvParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType

// Shim for package-private class UnivocityGenerator
class UnivocityGeneratorWrapper(schema: StructType, writer: Writer, options: CSVOptions)
    extends UnivocityGenerator(schema: StructType, writer: Writer, options: CSVOptions) {}

// Shim for package-private companion object UnivocityParser
object UnivocityParserUtils {
  def tokenizeStream(
      inputStream: InputStream,
      shouldDropHeader: Boolean,
      tokenizer: CsvParser): Iterator[Array[String]] = {
    UnivocityParser.tokenizeStream(inputStream, shouldDropHeader, tokenizer)
  }

  def parseIterator(
      lines: Iterator[String],
      parser: UnivocityParser,
      schema: StructType): Iterator[InternalRow] = {
    UnivocityParser.parseIterator(lines, parser, schema)
  }
}

// Shim for protected function makeSafeHeader
object DummyCsvDataSource extends CSVDataSource {
  override def isSplitable: Boolean = {
    throw new UnsupportedOperationException("Dummy CSV data source")
  }

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: UnivocityParser,
      requiredSchema: StructType,
      // Actual schema of data in the csv file
      dataSchema: StructType,
      caseSensitive: Boolean,
      columnPruning: Boolean): Iterator[InternalRow] = {
    throw new UnsupportedOperationException("Dummy CSV data source")
  }

  override def infer(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      parsedOptions: CSVOptions): StructType = {
    throw new UnsupportedOperationException("Dummy CSV data source")
  }

  def makeSafeHeaderShim(
      row: Array[String],
      caseSensitive: Boolean,
      options: CSVOptions): Array[String] = {
    makeSafeHeader(row, caseSensitive, options)
  }
}
