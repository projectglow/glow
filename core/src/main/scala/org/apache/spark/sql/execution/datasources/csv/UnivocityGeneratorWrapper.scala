package org.apache.spark.sql.execution.datasources.csv

import java.io.Writer

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

// Inline of protected function makeSafeHeader
object CSVDataSourceUtils {
  def makeSafeHeader(
      row: Array[String],
      caseSensitive: Boolean,
      options: CSVOptions): Array[String] = {
    if (options.headerFlag) {
      val duplicates = {
        val headerNames = row
          .filter(_ != null)
          .map(name => if (caseSensitive) name else name.toLowerCase)
        headerNames.diff(headerNames.distinct).distinct
      }

      row.zipWithIndex.map {
        case (value, index) =>
          if (value == null || value.isEmpty || value == options.nullValue) {
            // When there are empty strings or the values set in `nullValue`, put the
            // index as the suffix.
            s"_c$index"
          } else if (!caseSensitive && duplicates.contains(value.toLowerCase)) {
            // When there are case-insensitive duplicates, put the index as the suffix.
            s"$value$index"
          } else if (duplicates.contains(value)) {
            // When there are duplicates, put the index as the suffix.
            s"$value$index"
          } else {
            value
          }
      }
    } else {
      row.zipWithIndex.map {
        case (_, index) =>
          // Uses default column names, "_c#" where # is its position of fields
          // when header option is disabled.
          s"_c$index"
      }
    }
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
