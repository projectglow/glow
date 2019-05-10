package com.databricks.hls.sql.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.types.StructType

/**
 * A convenience class to help build converts from objects to Spark [[InternalRow]]s.
 *
 * @param schema The schema to which we're converting
 * @param fieldConverters Converters for each field in the schema
 * @param copy Whether to copy the buffer row before returning it. Empirically, this seems
 *             necessary when the rows are part of an array and unnecessary otherwise.
 * @tparam T
 */
class RowConverter[T](
    schema: StructType,
    fieldConverters: Array[RowConverter.Updater[T]],
    copy: Boolean = false)
    extends Function1[T, InternalRow] {

  private val container = new SpecificInternalRow(schema)
  private val projection = UnsafeProjection.create(schema)

  def apply(record: T): InternalRow = {
    var i = 0
    while (i < schema.length) {
      container.setNullAt(i)
      fieldConverters(i)(record, container, i)
      i += 1
    }

    if (copy) {
      projection(container).copy()
    } else {
      projection(container)
    }
  }
}

object RowConverter {
  type Updater[T] = (T, InternalRow, Int) => Unit
}
