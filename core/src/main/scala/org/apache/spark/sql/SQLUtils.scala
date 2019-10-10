package org.apache.spark.sql

import org.apache.spark.TaskContext
import org.apache.spark.ml.linalg.{MatrixUDT, VectorUDT}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

object SQLUtils {
  def verifyHasFields(schema: StructType, fields: Seq[StructField]): Unit = {
    fields.foreach { field =>
      val candidateFields = schema.fields.filter(_.name == field.name)
      if (candidateFields.length != 1) {
        throw new IllegalArgumentException(
          s"Schema must have exactly one field called ${field.name}. Current schema is $schema")
      }
      val schemaField = candidateFields.head
      if (!SQLUtils.structFieldsEqualExceptNullability(schemaField, field)) {
        throw new IllegalArgumentException(s"Schema must have a field $field")
      }
    }
  }

  def structFieldsEqualExceptNullability(f1: StructField, f2: StructField): Boolean = {
    f1.name == f2.name && f1.dataType.asNullable == f2.dataType.asNullable
  }

  def dataTypesEqualExceptNullability(dt1: DataType, dt2: DataType): Boolean = {
    dt1.asNullable == dt2.asNullable
  }

  /** Visibility hack to create DataFrames from internal rows */
  def internalCreateDataFrame(
      sess: SparkSession,
      catalystRows: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean): DataFrame = {

    sess.internalCreateDataFrame(catalystRows, schema, isStreaming)
  }

  /** Visibility shim to set the task context */
  def setTaskContext(context: TaskContext): Unit = {
    TaskContext.setTaskContext(context)
  }

  def newMatrixUDT(): MatrixUDT = {
    new MatrixUDT()
  }

  def newVectorUDT(): VectorUDT = {
    new VectorUDT()
  }

  def newAnalysisException(msg: String): AnalysisException = {
    new AnalysisException(msg)
  }

  type ADT = AbstractDataType
}
