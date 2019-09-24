package com.databricks.hls.sql.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.catalyst.expressions.{BoundReference, SpecificInternalRow}
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
    extends Function1[T, InternalRow]
    with Function2[T, InternalRow, InternalRow] {

  private val nullRow = new SpecificInternalRow(schema)
  private val exprs = schema
    .map(_.dataType)
    .zipWithIndex
    .map(x => BoundReference(x._2, x._1, true))
  private val projection = GenerateSafeProjection.generate(exprs)

  def apply(record: T): InternalRow = {
    nullRow.values.indices.foreach(nullRow.setNullAt)
    apply(record, nullRow)
  }

  // WARNING: this will modify priorRow that is passed in
  def apply(record: T, priorRow: InternalRow): InternalRow = {
    var i = 0
    while (i < schema.length) {
      fieldConverters(i)(record, priorRow, i)
      i += 1
    }
    if (copy) {
      projection(priorRow).copy()
    } else {
      projection(priorRow)
    }
  }
}

object RowConverter {
  type Updater[T] = (T, InternalRow, Int) => Unit
}
