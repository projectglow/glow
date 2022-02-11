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
import org.apache.spark.sql.SQLUtils.dataTypesEqualExceptNullability
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StringType

/**
 * A simple input formatter that writes each row as a string.
 */
class UTF8TextInputFormatter()
    extends InputFormatter[Option[org.apache.spark.unsafe.types.UTF8String]] {

  private var writer: PrintWriter = _

  override def init(stream: OutputStream): Unit = {
    writer = new PrintWriter(stream)
  }

  override def write(record: InternalRow): Unit = {
    value(record).foreach(writer.println) //scalastyle:ignore
  }

  override def value(record: InternalRow): Option[org.apache.spark.unsafe.types.UTF8String] = {
    if (!record.isNullAt(0)) {
      Some(record.getUTF8String(0))
    } else {
      None
    }
  }

  override def close(): Unit = {
    writer.close()
  }
}

class UTF8TextInputFormatterFactory extends InputFormatterFactory {
  override def name: String = "text"

  override def makeInputFormatter(df: DataFrame, options: Map[String, String])
      : InputFormatter[Option[org.apache.spark.unsafe.types.UTF8String]] = {
    require(df.schema.length == 1, "Input dataframe must have one column,")
    require(
      dataTypesEqualExceptNullability(df.schema.head.dataType, StringType),
      "Input dataframe must have one string column.")
    new UTF8TextInputFormatter
  }
}
