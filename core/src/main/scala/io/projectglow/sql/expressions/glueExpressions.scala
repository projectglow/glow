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

package io.projectglow.sql.expressions

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Alias, ExpectsInputTypes, Expression, Generator, GenericInternalRow, GetStructField, ImplicitCastInputTypes, NamedExpression, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.catalyst.{analysis, InternalRow}
import org.apache.spark.sql.types._

/**
 * Expands all the fields of a potentially unnamed struct.
 */
case class ExpandStruct(struct: Expression) extends Star with Unevaluable {
  override def expand(input: LogicalPlan, resolver: analysis.Resolver): Seq[NamedExpression] = {
    if (!struct.dataType.isInstanceOf[StructType]) {
      throw SQLUtils.newAnalysisException("Only structs can be expanded.")
    }

    struct.dataType.asInstanceOf[StructType].zipWithIndex.map {
      case (f, i) =>
        Alias(GetStructField(struct, i), f.name)()
    }
  }
}

/**
 * Expression that adds fields to an existing struct.
 *
 * At optimization time, this expression is rewritten as the creation of new struct with all the
 * fields of the existing struct as well as the new fields. See [[HLSReplaceExpressionsRule]]
 * for more details.
 */
case class AddStructFields(struct: Expression, newFields: Seq[Expression])
    extends Expression
    with Unevaluable {

  override def nullable: Boolean = true
  override def children: Seq[Expression] = struct +: newFields
  override def dataType: DataType = {
    var base = struct.dataType.asInstanceOf[StructType]
    newFields.grouped(2).foreach {
      case Seq(name, value) =>
        val nameStr = name.eval().toString
        base = base.add(nameStr, value.dataType, value.nullable)
    }
    base
  }
}

/**
 * Expression that rearranges and/or subsets an array by an array of indices.
 *
 * The logic of this expression is implemented in [[HLSReplaceExpressionsRule]].
 */
case class PermuteArray(dataArray: Expression, indexArray: Expression)
    extends Expression
    with Unevaluable {

  override def nullable: Boolean = true
  override def children: Seq[Expression] = Seq(dataArray, indexArray)
  override def dataType: DataType = dataArray.dataType

}

/**
 * Explodes a matrix by row. Each row of the input matrix will be output as an array of doubles.
 *
 * If the input expression is null or has 0 rows, the output will be empty.
 * @param matrixExpr The matrix to explode. May be dense or sparse.
 */
case class ExplodeMatrix(matrixExpr: Expression)
    extends Generator
    with CodegenFallback
    with ExpectsInputTypes {

  private val matrixUdt = SQLUtils.newMatrixUDT()

  override def children: Seq[Expression] = Seq(matrixExpr)

  override def elementSchema: StructType = {
    new StructType()
      .add("row", ArrayType(DoubleType, containsNull = false), nullable = false)
  }

  override def inputTypes = Seq(matrixUdt) // scalastyle:ignore

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val matrixStruct = matrixExpr.eval(input)
    if (matrixStruct == null) {
      return Iterator.empty
    }
    val matrix = matrixUdt.deserialize(matrixStruct).toDenseRowMajor
    var rowIdx = 0
    new Iterator[InternalRow] {
      override def hasNext: Boolean = rowIdx < matrix.numRows
      override def next(): InternalRow = {
        var colIdx = 0
        val arr = new Array[Any](matrix.numCols)
        while (colIdx < matrix.numCols) {
          arr(colIdx) = matrix.values(rowIdx * matrix.numCols + colIdx)
          colIdx += 1
        }
        rowIdx += 1
        new GenericInternalRow(Array[Any](new GenericArrayData(arr)))
      }
    }
  }
}

case class ArrayToSparseVector(child: Expression)
    extends UnaryExpression
    with ImplicitCastInputTypes {

  override def inputTypes: Seq[SQLUtils.ADT] = Seq(ArrayType(DoubleType))
  override def dataType: DataType = ArrayToSparseVector.vectorType
  override def nullSafeEval(input: Any): Any = ArrayToSparseVector.fromDoubleArray(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      c => {
        s"""
         |${ev.value} = 
         |io.projectglow.sql.expressions.ArrayToSparseVector.fromDoubleArray($c);
       """.stripMargin
      }
    )
  }
}

object ArrayToSparseVector {
  lazy val vectorType = SQLUtils.newVectorUDT()

  def fromDoubleArray(input: Any): InternalRow = {
    val vector = Vectors.dense(input.asInstanceOf[ArrayData].toDoubleArray())
    vectorType.serialize(vector.toSparse)
  }
}

case class ArrayToDenseVector(child: Expression)
    extends UnaryExpression
    with ImplicitCastInputTypes {

  override def inputTypes: Seq[SQLUtils.ADT] = Seq(ArrayType(DoubleType))
  override def dataType: DataType = ArrayToDenseVector.vectorType
  override def nullSafeEval(input: Any): Any = ArrayToDenseVector.fromDoubleArray(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      c => {
        s"""
         |${ev.value} = 
         |io.projectglow.sql.expressions.ArrayToDenseVector.fromDoubleArray($c);
       """.stripMargin
      }
    )
  }
}

object ArrayToDenseVector {
  private lazy val vectorType = SQLUtils.newVectorUDT()

  def fromDoubleArray(input: Any): InternalRow = {
    val vector = Vectors.dense(input.asInstanceOf[ArrayData].toDoubleArray())
    vectorType.serialize(vector)
  }
}

case class VectorToArray(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def inputTypes: Seq[SQLUtils.ADT] = Seq(VectorToArray.vectorType)
  override def dataType: DataType = ArrayType(DoubleType)
  override def nullSafeEval(input: Any): Any = VectorToArray.toDoubleArray(input)
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => {
      s"""
         |${ev.value} = 
         |io.projectglow.sql.expressions.VectorToArray.toDoubleArray($c);
       """.stripMargin
    })
  }
}

object VectorToArray {
  lazy val vectorType = SQLUtils.newVectorUDT()
  def toDoubleArray(input: Any): ArrayData = {
    new GenericArrayData(vectorType.deserialize(input).toArray)
  }
}
