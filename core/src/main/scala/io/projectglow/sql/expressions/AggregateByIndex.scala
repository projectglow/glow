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

import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, DeclarativeAggregate}
import org.apache.spark.sql.types._

/**
 * An expression that allows users to aggregate over all array elements at a specific index in an
 * array column. For example, this expression can be used to compute per-sample summary statistics
 * from a genotypes column.
 *
 * The user must provide the following arguments:
 * - The array for aggregation
 * - The initialValue for each element in the per-index buffer
 * - An update function to update the accumulator with a new element
 * - A merge function to combine two accumulators
 *
 * The user may optionally provide an evaluate function. If it's not provided, the identity function
 * is used.
 *
 * Example usage to calculate total depth across all sites for a sample:
 * aggregate_by_index(
 *   genotypes,
 *   0,
 *   (acc, genotype) -> acc + genotype.depth,
 *   (acc1, acc2) -> acc1 + acc2,
 *   acc -> acc)
 */
trait AggregateByIndex extends DeclarativeAggregate with HigherOrderFunction {

  /** The array over which we're aggregating */
  def arr: Expression

  /** The initial value for each element in the aggregation buffer */
  def initialValue: Expression

  /** Function to update an element of the buffer with a new input element */
  def update: Expression

  /** Function to merge two buffers elements */
  def merge: Expression

  /** Function to turn a buffer into the actual output */
  def evaluate: Expression

  def withBoundExprs(
      newUpdate: Expression,
      newMerge: Expression,
      newEvaluate: Expression): AggregateByIndex

  protected val buffer: AttributeReference = {
    AttributeReference("buffer", ArrayType(initialValue.dataType))()
  }

  /**
   * Since we don't know the proper size of the aggregation buffer, we create it with the size
   * of the data array (or the aggregation buffer with which it's being merged) if it's not yet
   * defined.
   */
  private def bufferNotNull(buf: Expression, array: Expression): Expression = {
    If(buf.isNull, ArrayRepeat(initialValue, Size(array)), buf)
  }

  override def prettyName: String = "aggregate_by_index"
  override def nullable: Boolean = children.exists(_.nullable)
  override def dataType: DataType = ArrayType(evaluate.dataType)
  override def functions: Seq[Expression] = Seq(update, merge, evaluate)
  override def functionTypes: Seq[SQLUtils.ADT] = {
    Seq(SQLUtils.anyDataType, SQLUtils.anyDataType, SQLUtils.anyDataType)
  }

  override def arguments: Seq[Expression] = Seq(arr)
  override def argumentTypes: Seq[SQLUtils.ADT] = Seq(arr.dataType)

  /**
   * To create bound lambda functions, we bind each of the child higher order functions,
   * extract their bound lambda functions, and then copy.
   */
  override def bind(
      f: (Expression, Seq[(DataType, Boolean)]) => LambdaFunction): AggregateByIndex = {
    withBoundExprs(
      updateExpr.bind(f).function,
      mergeExpr.bind(f).function,
      evaluateExpr.bind(f).function
    )
  }

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(buffer)

  private lazy val updateExpr = {
    ZipWith(bufferNotNull(buffer, arr), arr, update)
  }

  private lazy val mergeExpr = {
    val leftBuffer = bufferNotNull(buffer.left, buffer.right)
    val rightBuffer = bufferNotNull(buffer.right, buffer.left)

    ZipWith(leftBuffer, rightBuffer, merge)
  }

  private lazy val evaluateExpr = ArrayTransform(buffer, evaluate)

  override lazy val evaluateExpression: Expression = evaluateExpr

  override lazy val initialValues: Seq[Expression] = {
    Seq(Literal(null, ArrayType(initialValue.dataType)))
  }

  override lazy val mergeExpressions: Seq[Expression] = Seq(mergeExpr)

  override lazy val updateExpressions: Seq[Expression] = Seq(updateExpr)
}

/**
 * A hack to make Spark SQL recognize [[AggregateByIndex]] as an aggregate expression.
 *
 * See [[io.projectglow.sql.optimizer.ResolveAggregateFunctionsRule]] for details.
 */
trait UnwrappedAggregateFunction extends AggregateFunction {
  def asWrapped: AggregateFunction
}

case class UnwrappedAggregateByIndex(
    arr: Expression,
    initialValue: Expression,
    update: Expression,
    merge: Expression,
    evaluate: Expression)
    extends AggregateByIndex
    with UnwrappedAggregateFunction {

  override def withBoundExprs(
      newUpdate: Expression,
      newMerge: Expression,
      newEvaluate: Expression): AggregateByIndex = {

    copy(update = newUpdate, merge = newMerge, evaluate = newEvaluate)
  }

  override def asWrapped: AggregateFunction = {
    WrappedAggregateByIndex(arr, initialValue, update, merge, evaluate)
  }
}

case class WrappedAggregateByIndex(
    arr: Expression,
    initialValue: Expression,
    update: Expression,
    merge: Expression,
    evaluate: Expression)
    extends AggregateByIndex {

  override def withBoundExprs(
      newUpdate: Expression,
      newMerge: Expression,
      newEvaluate: Expression): AggregateByIndex = {

    copy(update = newUpdate, merge = newMerge, evaluate = newEvaluate)
  }
}
