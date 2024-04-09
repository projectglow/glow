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

import io.projectglow.sql.util.{Rewrite, RewriteAfterResolution}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, ArraySort, BinaryExpression, CaseWhen, Cast, Ceil, CreateNamedStruct, Divide, EqualTo, Exp, ExpectsInputTypes, Expression, Factorial, Floor, Generator, GenericInternalRow, GetArrayItem, GetStructField, Greatest, If, ImplicitCastInputTypes, Least, LessThan, Literal, Log, Multiply, NamedExpression, Pi, Round, Size, Subtract, UnaryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import io.projectglow.SparkShim.newUnresolvedException

/**
 * Expands all the fields of a potentially unnamed struct.
 */
case class ExpandStruct(struct: Expression) extends Expression with Unevaluable {
  override def children: Seq[Expression] = Seq(struct)
  override lazy val resolved: Boolean = false
  override def dataType: DataType = throw newUnresolvedException(this, "dataType")
  override def nullable: Boolean = throw newUnresolvedException(this, "nullable")
  def expand(): Seq[NamedExpression] = {
    if (!struct.dataType.isInstanceOf[StructType]) {
      throw new IllegalArgumentException("Only structs can be expanded.")
    }

    struct.dataType.asInstanceOf[StructType].zipWithIndex.map { case (f, i) =>
      Alias(GetStructField(struct, i), f.name)()
    }
  }

  protected def withNewChildrenInternal(children: IndexedSeq[Expression]): ExpandStruct = {
    copy(struct = children.head)
  }
}

case class SubsetStruct(struct: Expression, fields: Seq[Expression]) extends Rewrite {
  override def children: Seq[Expression] = Seq(struct) ++ fields

  override def rewrite: Expression = {
    CreateNamedStruct(fields.flatMap(f => Seq(f, UnresolvedExtractValue(struct, f))))
  }

  protected def withNewChildrenInternal(children: IndexedSeq[Expression]): SubsetStruct = {
    copy(struct = children.head)
  }
}

/**
 * Expression that adds fields to an existing struct.
 *
 * At optimization time, this expression is rewritten as the creation of new struct with all the
 * fields of the existing struct as well as the new fields. See [[io.projectglow.sql.optimizer.ReplaceExpressionsRule]]
 * for more details.
 */
case class AddStructFields(struct: Expression, newFields: Seq[Expression])
    extends RewriteAfterResolution {
  override def children: Seq[Expression] = struct +: newFields
  override def rewrite: Expression = {
    val baseType = struct.dataType.asInstanceOf[StructType]
    val baseFields = baseType.indices.flatMap { idx =>
      Seq(Literal(baseType(idx).name), GetStructField(struct, idx))
    }
    CreateNamedStruct(baseFields ++ newFields)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): AddStructFields =
    copy(struct = newChildren(0), newFields = newChildren.drop(1))
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

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ExplodeMatrix =
    copy(matrixExpr = newChildren.head)
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

  protected def withNewChildInternal(newChild: Expression): ArrayToSparseVector = {
    copy(child = newChild)
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

  protected def withNewChildInternal(newChild: Expression): ArrayToDenseVector = {
    copy(child = newChild)
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
    nullSafeCodeGen(
      ctx,
      ev,
      c => {
        s"""
         |${ev.value} =
         |io.projectglow.sql.expressions.VectorToArray.toDoubleArray($c);
       """.stripMargin
      })
  }

  protected def withNewChildInternal(newChild: Expression): VectorToArray = {
    copy(child = newChild)
  }
}

object VectorToArray {
  lazy val vectorType = SQLUtils.newVectorUDT()
  def toDoubleArray(input: Any): ArrayData = {
    new GenericArrayData(vectorType.deserialize(input).toArray)
  }
}

case class Comb(n: Expression, k: Expression) extends RewriteAfterResolution {
  override def children: Seq[Expression] = Seq(n, k)

  override def rewrite: Expression = {
    Cast(
      Round(
        Exp(Subtract(Subtract(LogFactorial(n), LogFactorial(k)), LogFactorial(Subtract(n, k)))),
        Literal(0)),
      LongType)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(n = newChildren(0), k = newChildren(1))
  }
}

/**
 * Note: not user facing, approximate for n > 47
 */
case class LogFactorial(n: Expression) extends RewriteAfterResolution {
  override def children: Seq[Expression] = Seq(n)

  override def rewrite: Expression = {
    Add
    CaseWhen(
      Seq(
        (EqualTo(n, Literal(0)), Literal(0.0)),
        (EqualTo(n, Literal(1)), Literal(0.0)),
        (EqualTo(n, Literal(2)), Literal(0.693147180559945)),
        (EqualTo(n, Literal(3)), Literal(1.7917594692280554)),
        (EqualTo(n, Literal(4)), Literal(3.178053830347945)),
        (EqualTo(n, Literal(5)), Literal(4.787491742782047)),
        (EqualTo(n, Literal(6)), Literal(6.579251212010102)),
        (EqualTo(n, Literal(7)), Literal(8.525161361065415)),
        (EqualTo(n, Literal(8)), Literal(10.604602902745249)),
        (EqualTo(n, Literal(9)), Literal(12.801827480081467)),
        (EqualTo(n, Literal(10)), Literal(15.104412573075514)),
        (EqualTo(n, Literal(11)), Literal(17.502307845873887)),
        (EqualTo(n, Literal(12)), Literal(19.987214495661885)),
        (EqualTo(n, Literal(13)), Literal(22.55216385312342)),
        (EqualTo(n, Literal(14)), Literal(25.191221182738683)),
        (EqualTo(n, Literal(15)), Literal(27.89927138384089)),
        (EqualTo(n, Literal(16)), Literal(30.671860106080672)),
        (EqualTo(n, Literal(17)), Literal(33.50507345013689)),
        (EqualTo(n, Literal(18)), Literal(36.39544520803305)),
        (EqualTo(n, Literal(19)), Literal(39.339884187199495)),
        (EqualTo(n, Literal(20)), Literal(42.335616460753485)),
        (EqualTo(n, Literal(21)), Literal(45.38013889847691)),
        (EqualTo(n, Literal(22)), Literal(48.47118135183522)),
        (EqualTo(n, Literal(23)), Literal(51.60667556776438)),
        (EqualTo(n, Literal(24)), Literal(54.78472939811232)),
        (EqualTo(n, Literal(25)), Literal(58.00360522298052)),
        (EqualTo(n, Literal(26)), Literal(61.26170176100201)),
        (EqualTo(n, Literal(27)), Literal(64.55753862700634)),
        (EqualTo(n, Literal(28)), Literal(67.88974313718153)),
        (EqualTo(n, Literal(29)), Literal(71.257038967168)),
        (EqualTo(n, Literal(30)), Literal(74.65823634883017)),
        (EqualTo(n, Literal(31)), Literal(78.0922235533153)),
        (EqualTo(n, Literal(32)), Literal(81.55795945611503)),
        (EqualTo(n, Literal(33)), Literal(85.05446701758152)),
        (EqualTo(n, Literal(34)), Literal(88.58082754219768)),
        (EqualTo(n, Literal(35)), Literal(92.1361756036871)),
        (EqualTo(n, Literal(36)), Literal(95.7196945421432)),
        (EqualTo(n, Literal(37)), Literal(99.33061245478743)),
        (EqualTo(n, Literal(38)), Literal(102.96819861451381)),
        (EqualTo(n, Literal(39)), Literal(106.63176026064346)),
        (EqualTo(n, Literal(40)), Literal(110.32063971475738)),
        (EqualTo(n, Literal(41)), Literal(114.03421178146169)),
        (EqualTo(n, Literal(42)), Literal(117.77188139974507)),
        (EqualTo(n, Literal(43)), Literal(121.53308151543864)),
        (EqualTo(n, Literal(44)), Literal(125.3172711493569)),
        (EqualTo(n, Literal(45)), Literal(129.12393363912722)),
        (EqualTo(n, Literal(46)), Literal(132.95257503561632)),
        (EqualTo(n, Literal(47)), Literal(136.80272263732635))
      ),
      Some(
        Add(
          Subtract(Multiply(Add(n, Literal(0.5)), Log(n)), n),
          Multiply(Literal(0.5), Log(Multiply(Literal(2), Pi())))))
    )
  }
  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(n = newChildren(0))
  }
}

case class ArrayQuantile(arr: Expression, probability: Expression, isSorted: Expression)
    extends RewriteAfterResolution {

  def this(arr: Expression, probability: Expression) = this(arr, probability, Literal(false))
  override def children: Seq[Expression] = Seq(arr, probability, isSorted)

  private def getQuantile(arr: Expression): Expression = {
    val trueIndex = Add(Multiply(probability, Subtract(Size(arr), Literal(1))), Literal(1))
    val roundedIdx = Cast(trueIndex, IntegerType)
    val below = GetArrayItem(arr, Greatest(Seq(Literal(0), Subtract(roundedIdx, Literal(1)))))
    val above = GetArrayItem(arr, Least(Seq(Subtract(Size(arr), Literal(1)), roundedIdx)))
    val frac = Subtract(trueIndex, roundedIdx)
    Add(Multiply(frac, above), Multiply(Subtract(Literal(1), frac), below))
  }

  override def rewrite: Expression = {
    If(isSorted, getQuantile(arr), getQuantile(new ArraySort(arr)))
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(arr = newChildren(0), probability = newChildren(1), isSorted = newChildren(2))
  }
}
