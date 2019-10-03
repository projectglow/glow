package com.databricks.hls.sql.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType

/**
 * A convenience class to help build converts from objects to Spark [[InternalRow]]s.
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
