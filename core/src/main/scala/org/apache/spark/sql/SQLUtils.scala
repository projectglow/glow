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

  def anyDataType: ADT = AnyDataType

  type ADT = AbstractDataType

  def getSessionExtensions(session: SparkSession): SparkSessionExtensions = {
    session.extensions
  }

  // Used to create an empty RDD with 1 partition to be compatible with our partition-based functionality
  def createEmptyRDD(sess: SparkSession, numPartitions: Int = 1): RDD[InternalRow] = {
    sess.sparkContext.parallelize(Seq.empty[InternalRow], numPartitions)
  }
}
