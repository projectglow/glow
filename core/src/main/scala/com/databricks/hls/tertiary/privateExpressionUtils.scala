package org.apache.spark.sql.databricks.hls.tertiary // Visibility hack

import org.apache.spark.ml.linalg.MatrixUDT
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Alias, ExpectsInputTypes, Expression, Generator, GenericInternalRow, GetStructField, NamedExpression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.{analysis, InternalRow}
import org.apache.spark.sql.types._

/**
 * Expands all the fields of a potentially unnamed struct.
 */
case class ExpandStruct(struct: Expression) extends Star with Unevaluable {
  override def expand(input: LogicalPlan, resolver: analysis.Resolver): Seq[NamedExpression] = {
    if (!struct.dataType.isInstanceOf[StructType]) {
      throw new AnalysisException("Only structs can be expanded.")
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
 * Explodes a matrix by row. Each row of the input matrix will be output as an array of doubles.
 *
 * If the input expression is null or has 0 rows, the output will be empty.
 * @param matrixExpr The matrix to explode. May be dense or sparse.
 */
case class ExplodeMatrix(matrixExpr: Expression)
    extends Generator
    with CodegenFallback
    with ExpectsInputTypes {

  val matrixUdt = new MatrixUDT()

  override def children: Seq[Expression] = Seq(matrixExpr)

  override def elementSchema: StructType = {
    new StructType()
      .add("row", ArrayType(DoubleType, containsNull = false), nullable = false)
  }

  override def inputTypes: Seq[AbstractDataType] = Seq(matrixUdt)

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
