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

package io.projectglow.transformers.pipe

import java.io.InputStream

import scala.collection.JavaConverters._

import com.univocity.parsers.csv.CsvParser
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.execution.datasources.csv.{CSVDataSourceUtils, CSVUtils, UnivocityParserUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import io.projectglow.SparkShim.{CSVOptions, UnivocityParser}

class CSVOutputFormatter(parsedOptions: CSVOptions) extends OutputFormatter {

  private def getSchema(record: Array[String]): StructType = {
    val header =
      CSVDataSourceUtils.makeSafeHeader(
        record,
        SQLConf.get.caseSensitiveAnalysis,
        parsedOptions
      )
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

    if (filteredLines.isEmpty) {
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
        schema
      )

    val parsedIterWithoutHeader = if (parsedOptions.headerFlag) {
      parsedIter.drop(1)
    } else {
      parsedIter
    }

    Iterator(schema) ++ parsedIterWithoutHeader.map(_.copy)
  }
}

class CSVOutputFormatterFactory extends OutputFormatterFactory {
  override def name: String = "csv"

  override def makeOutputFormatter(
      options: Map[String, String]
  ): OutputFormatter = {
    val parsedOptions =
      new CSVOptions(
        options,
        SQLConf.get.csvColumnPruning,
        SQLConf.get.sessionLocalTimeZone
      )
    new CSVOutputFormatter(parsedOptions)
  }
}
