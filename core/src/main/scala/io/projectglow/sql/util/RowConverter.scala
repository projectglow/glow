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

package io.projectglow.sql.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType

/**
 * A convenience class to help convert from objects to Spark [[InternalRow]]s.
 *
 * @param schema The schema to which we're converting
 * @param fieldConverters Converters for each field in the schema
 * @tparam T
 */
class RowConverter[T](schema: StructType, fieldConverters: Array[RowConverter.Updater[T]]) {

  def apply(record: T): InternalRow = {
    val nullRow = new GenericInternalRow(schema.length)
    apply(record, nullRow)
  }

  // WARNING: this will modify priorRow that is passed in
  def apply(record: T, priorRow: InternalRow): InternalRow = {
    var i = 0
    while (i < schema.length) {
      fieldConverters(i)(record, priorRow, i)
      i += 1
    }

    priorRow
  }
}

object RowConverter {
  type Updater[T] = (T, InternalRow, Int) => Unit
}
