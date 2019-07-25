package org.apache.spark.sql.execution.datasources.csv

import java.io.{InputStream, Writer}

import com.univocity.parsers.csv.CsvParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{BadRecordException, DropMalformedMode, FailFastMode, ParseMode, PermissiveMode}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

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

  // Inline of parseIterator from Spark 2.4 to avoid signature incompatibilities
  def parseIterator(
      lines: Iterator[String],
      parser: UnivocityParser,
      schema: StructType): Iterator[InternalRow] = {
    val options = parser.options
    val filteredLines = CSVUtils.filterCommentAndEmpty(lines, options)
    val safeParser = new MinimalFailureSafeParser[String](
      input => Seq(parser.parse(input)),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)
    filteredLines.flatMap(safeParser.parse)
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

// Inline of FailureSafeParser from Spark 2.4 to avoid signature incompatibilities
class MinimalFailureSafeParser[IN](
    rawParser: IN => Seq[InternalRow],
    mode: ParseMode,
    schema: StructType,
    columnNameOfCorruptRecord: String) {

  private val corruptFieldIndex = schema.getFieldIndex(columnNameOfCorruptRecord)
  private val actualSchema = StructType(schema.filterNot(_.name == columnNameOfCorruptRecord))
  private val resultRow = new GenericInternalRow(schema.length)
  private val nullResult = new GenericInternalRow(schema.length)

  // This function takes 2 parameters: an optional partial result, and the bad record. If the given
  // schema doesn't contain a field for corrupted record, we just return the partial result or a
  // row with all fields null. If the given schema contains a field for corrupted record, we will
  // set the bad record to this field, and set other fields according to the partial result or null.
  private val toResultRow: (Option[InternalRow], () => UTF8String) => InternalRow = {
    if (corruptFieldIndex.isDefined) { (row, badRecord) =>
      {
        var i = 0
        while (i < actualSchema.length) {
          val from = actualSchema(i)
          resultRow(schema.fieldIndex(from.name)) = row.map(_.get(i, from.dataType)).orNull
          i += 1
        }
        resultRow(corruptFieldIndex.get) = badRecord()
        resultRow
      }
    } else { (row, _) =>
      row.getOrElse(nullResult)
    }
  }

  def parse(input: IN): Iterator[InternalRow] = {
    try {
      rawParser.apply(input).toIterator.map(row => toResultRow(Some(row), () => null))
    } catch {
      case e: BadRecordException =>
        mode match {
          case PermissiveMode =>
            Iterator(toResultRow(e.partialResult(), e.record))
          case DropMalformedMode =>
            Iterator.empty
          case FailFastMode =>
            throw new SparkException(
              "Malformed records are detected in record parsing. " +
              s"Parse Mode: ${FailFastMode.name}.",
              e.cause)
        }
    }
  }
}
