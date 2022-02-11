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

import java.io.{OutputStream, PrintWriter}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.csv.SGUnivocityGenerator
import org.apache.spark.sql.types.StructType

import io.projectglow.SparkShim.CSVOptions

class CSVInputFormatter(schema: StructType, parsedOptions: CSVOptions)
    extends InputFormatter[InternalRow] {

  private var writer: PrintWriter = _
  private var univocityGenerator: SGUnivocityGenerator = _

  override def init(stream: OutputStream): Unit = {
    writer = new PrintWriter(stream)
    univocityGenerator = new SGUnivocityGenerator(schema, writer, parsedOptions)
    if (parsedOptions.headerFlag) {
      univocityGenerator.writeHeaders()
    }
  }

  override def write(record: InternalRow): Unit = {
    univocityGenerator.write(value(record))
  }

  override def value(record: InternalRow): InternalRow = record

  override def close(): Unit = {
    writer.close()
    univocityGenerator.close()
  }
}

class CSVInputFormatterFactory extends InputFormatterFactory {
  override def name: String = "csv"

  override def makeInputFormatter(
      df: DataFrame,
      options: Map[String, String]
  ): InputFormatter[InternalRow] = {
    val sqlConf = df.sparkSession.sessionState.conf
    val parsedOptions =
      new CSVOptions(
        options,
        sqlConf.csvColumnPruning,
        sqlConf.sessionLocalTimeZone
      )
    new CSVInputFormatter(df.schema, parsedOptions)
  }
}
