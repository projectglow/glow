/*
 * Copyright 2019 The Glow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.csv

import java.io.Writer

import com.univocity.parsers.csv.CsvWriter
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.SparkShim.{CSVOptions => ShimCSVOptions, UnivocityParser => ShimUnivocityParser, wrapUnivocityParse}

/**
 * Inlined version of [[UnivocityGenerator]] to handle compatibility between Spark distributions.
 */
class SGUnivocityGenerator(schema: StructType, writer: Writer, options: ShimCSVOptions) {
  private val writerSettings = options.asWriterSettings
  writerSettings.setHeaders(schema.fieldNames: _*)
  private val gen = new CsvWriter(writer, writerSettings)

  // A `ValueConverter` is responsible for converting a value of an `InternalRow` to `String`.
  // When the value is null, this converter should not be called.
  private type ValueConverter = (InternalRow, Int) => String

  // `ValueConverter`s for all values in the fields of the schema
  private val valueConverters: Array[ValueConverter] =
    schema.map(_.dataType).map(makeConverter).toArray

  private def makeConverter(dataType: DataType): ValueConverter =
    dataType match {
      case DateType =>
        (row: InternalRow, ordinal: Int) =>
          options
            .dateFormat
            .format(
              DateTimeUtils.toJavaDate(row.getInt(ordinal))
            )

      case TimestampType =>
        (row: InternalRow, ordinal: Int) =>
          options
            .timestampFormat
            .format(
              DateTimeUtils.toJavaTimestamp(row.getLong(ordinal))
            )

      case udt: UserDefinedType[_] => makeConverter(udt.sqlType)

      case dt: DataType =>
        (row: InternalRow, ordinal: Int) => row.get(ordinal, dt).toString
    }

  private def convertRow(row: InternalRow): Seq[String] = {
    var i = 0
    val values = new Array[String](row.numFields)
    while (i < row.numFields) {
      if (!row.isNullAt(i)) {
        values(i) = valueConverters(i).apply(row, i)
      } else {
        values(i) = options.nullValue
      }
      i += 1
    }
    values
  }

  def writeHeaders(): Unit = {
    gen.writeHeaders()
  }

  /**
   * Writes a single InternalRow to CSV using Univocity.
   */
  def write(row: InternalRow): Unit = {
    gen.writeRow(convertRow(row): _*)
  }

  def close(): Unit = gen.close()

  def flush(): Unit = gen.flush()
}

// Shim for package-private companion object UnivocityParser
object UnivocityParserUtils {

  // Inline of parseIterator from Spark 2.4 to avoid signature incompatibilities
  // Accepts pre-filtered lines (using CSVUtils.filterCommentAndEmpty)
  def parseIterator(
      filteredLines: Iterator[String],
      parser: ShimUnivocityParser,
      schema: StructType): Iterator[InternalRow] = {
    val safeParser = new MinimalFailureSafeParser[String](
      input => wrapUnivocityParse(parser)(input).toSeq,
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord
    )
    filteredLines.flatMap(safeParser.parse)
  }
}

// Inline of protected function makeSafeHeader
object CSVDataSourceUtils {
  def makeSafeHeader(
      row: Array[String],
      caseSensitive: Boolean,
      options: ShimCSVOptions): Array[String] = {
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

  private val corruptFieldIndex =
    schema.getFieldIndex(columnNameOfCorruptRecord)
  private val actualSchema = StructType(
    schema.filterNot(_.name == columnNameOfCorruptRecord)
  )
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
      rawParser
        .apply(input)
        .toIterator
        .map(row => toResultRow(Some(row), () => null))
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
              e.cause
            )
        }
    }
  }
}
