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

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A simple output formatter that returns each line of output as a String field.
 */
class UTF8TextOutputFormatter() extends OutputFormatter {

  override def makeIterator(stream: InputStream): Iterator[Any] = {
    val schema = StructType(Seq(StructField("text", StringType)))
    val iter = IOUtils.lineIterator(stream, "UTF-8").asScala.map { s =>
      new GenericInternalRow(Array(UTF8String.fromString(s)): Array[Any])
    }
    Iterator(schema) ++ iter
  }
}

class UTF8TextOutputFormatterFactory extends OutputFormatterFactory {
  override def name: String = "text"

  override def makeOutputFormatter(options: Map[String, String]): OutputFormatter = {
    new UTF8TextOutputFormatter
  }
}
