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
 * - An update function to update the buffer with a new element
 * - A merge function to combine two buffers
 *
 * The user may optionally provide an evaluate function. If it's not provided, the identity function
 * is used.
 *
 * Example usage to calculate average depth across all sites for a sample:
 * aggregate_by_index(
 *   genotypes,
 *   named_struct('sum', 0l, 'count', 0l),
 *   (buf, genotype) -> named_struct('sum', buf.sum + genotype.depth, 'count', buf.count + 1),
 *   (buf1, buf2) -> named_struct('sum', buf1.sum + buf2.sum, 'count', buf1.count + buf2.count),
 *   buf -> buf.sum / buf.count)
 */
trait AggregateByIndex extends DeclarativeAggregate with HigherOrderFunction {

  // Fields specific to AggregateByIndex that must be overridden by subclasses
  /** The array over which we're aggregating */
  def arr: Expression

  /** The initial value for each element in the aggregation buffer */
  def initialValue: Expression

  /** Function to update an element of the buffer with a new input element */
  def update: Expression

  /** Function to merge two buffers' elements */
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

  // Overrides from [[Expression]]
  override def prettyName: String = "aggregate_by_index"
  override def nullable: Boolean = children.exists(_.nullable)
  override def dataType: DataType = ArrayType(evaluate.dataType)

  // Overrides from [[HigherOrderFunction]]
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

  // Overrides from [[DeclarativeAggregate]]
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

  /**
   * A helper for accessing a buffer that may not yet be initialized.
   *
   * @param buf The maybe initialized buffer to return
   * @param array An input array that has the same length as the aggregation buffer
   * @return `buf` if it is not null, otherwise [[initialValue]] repeated size(`array`) times
   */
  private def bufferNotNull(buf: Expression, array: Expression): Expression = {
    If(buf.isNull, ArrayRepeat(initialValue, Size(array)), buf)
  }
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

  def this(arr: Expression, initialValue: Expression, update: Expression, merge: Expression) = {
    this(arr, initialValue, update, merge, LambdaFunction.identity)
  }

  override def children: Seq[Expression] = arguments ++ functions

  override def prettyName: String = "unwrapped_aggregate_by_index"

  override def withBoundExprs(
      newUpdate: Expression,
      newMerge: Expression,
      newEvaluate: Expression): AggregateByIndex = {

    copy(update = newUpdate, merge = newMerge, evaluate = newEvaluate)
  }

  override def asWrapped: AggregateFunction = {
    WrappedAggregateByIndex(arr, initialValue, update, merge, evaluate)
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): UnwrappedAggregateByIndex =
    copy(
      arr = newChildren(0),
      update = newChildren(1),
      merge = newChildren(2),
      evaluate = newChildren(3))
}

case class WrappedAggregateByIndex(
    arr: Expression,
    initialValue: Expression,
    update: Expression,
    merge: Expression,
    evaluate: Expression = LambdaFunction.identity)
    extends AggregateByIndex {

  override def children: Seq[Expression] = arguments ++ functions

  override def prettyName: String = "wrapped_agg_by"

  override def withBoundExprs(
      newUpdate: Expression,
      newMerge: Expression,
      newEvaluate: Expression): AggregateByIndex = {

    copy(update = newUpdate, merge = newMerge, evaluate = newEvaluate)
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): WrappedAggregateByIndex =
    copy(
      arr = newChildren(0),
      update = newChildren(1),
      merge = newChildren(2),
      evaluate = newChildren(3))
}
