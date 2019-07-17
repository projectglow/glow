package org.apache.spark.sql

import org.apache.spark.ml.linalg.{VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

object SQLUtils {
  def structFieldsEqualExceptNullability(f1: StructField, f2: StructField): Boolean = {
    f1.name == f2.name && f1.dataType.asNullable == f2.dataType.asNullable
  }

  def dataTypesEqualExceptNullability(dt1: DataType, dt2: DataType): Boolean = {
    dt1.asNullable == dt2.asNullable
  }

  /** Visibility hack to create DataFrames from internal rows */
  def createDataFrame(
      sess: SparkSession,
      catalystRows: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean = false): DataFrame = {

    sess.internalCreateDataFrame(catalystRows, schema, isStreaming)
  }
}

case class ArrayToSparseVector(child: Expression)
    extends UnaryExpression
    with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType(DoubleType))
  override def dataType: DataType = ArrayToSparseVector.vectorType
  override def nullSafeEval(input: Any): Any = ArrayToSparseVector.fromDoubleArray(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => {
      s"""
         |${ev.value} = org.apache.spark.sql.ArrayToSparseVector.fromDoubleArray($c);
       """.stripMargin
    })
  }
}

object ArrayToSparseVector {
  lazy val vectorType: VectorUDT = new VectorUDT()

  def fromDoubleArray(input: Any): InternalRow = {
    val vector = Vectors.dense(input.asInstanceOf[ArrayData].toDoubleArray())
    vectorType.serialize(vector.toSparse)
  }
}

case class ArrayToDenseVector(child: Expression)
    extends UnaryExpression
    with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(ArrayType(DoubleType))
  override def dataType: DataType = ArrayToDenseVector.vectorType
  override def nullSafeEval(input: Any): Any = ArrayToDenseVector.fromDoubleArray(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => {
      s"""
         |${ev.value} = org.apache.spark.sql.ArrayToDenseVector.fromDoubleArray($c);
       """.stripMargin
    })
  }
}

object ArrayToDenseVector {
  lazy val vectorType: VectorUDT = new VectorUDT()

  def fromDoubleArray(input: Any): InternalRow = {
    val vector = Vectors.dense(input.asInstanceOf[ArrayData].toDoubleArray())
    vectorType.serialize(vector)
  }
}

case class VectorToArray(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[AbstractDataType] = Seq(VectorToArray.vectorType)
  override def dataType: DataType = ArrayType(DoubleType)
  override def nullSafeEval(input: Any): Any = VectorToArray.toDoubleArray(input)
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => {
      s"""
         |${ev.value} = org.apache.spark.sql.VectorToArray.toDoubleArray($c);
       """.stripMargin
    })
  }
}

object VectorToArray {
  lazy val vectorType: VectorUDT = new VectorUDT()
  def toDoubleArray(input: Any): ArrayData = {
    new GenericArrayData(vectorType.deserialize(input).toArray)
  }
}
