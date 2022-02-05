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

import java.io.File

import htsjdk.samtools.liftover.LiftOver
import htsjdk.samtools.util.Interval
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Performs lift over from the specified 0-start, half-open interval (contigName, start, end) on the reference sequence
 * to a query sequence, using the specified chain file and minimum fraction of bases that must remap.
 *
 * We assume the chain file is a constant value so that the LiftOver object can be reused between rows.
 *
 * If any of the required parameters (contigName, start, end) are null, the expression returns null.
 * If minMatchRatioOpt contains null, the expression returns null; if it is empty, we use 0.95 to match
 * [[LiftOver.DEFAULT_LIFTOVER_MINMATCH]].
 *
 * @param contigName Chromosome name on the reference sequence.
 * @param start Start position (0-start) on the reference sequence.
 * @param end End position on the reference sequence.
 * @param chainFile UCSC chain format file mapping blocks from the reference sequence to the query sequence.
 * @param minMatchRatio The minimum fraction of bases that must remap to lift over successfully.
 */
case class LiftOverCoordinatesExpr(
    contigName: Expression,
    start: Expression,
    end: Expression,
    chainFile: Expression,
    minMatchRatio: Expression)
    extends CodegenFallback
    with ImplicitCastInputTypes {

  def this(contigName: Expression, start: Expression, end: Expression, chainFile: Expression) = {
    this(contigName, start, end, chainFile, Literal(0.95d))
  }

  private lazy val liftOver = new LiftOver(
    new File(chainFile.eval().asInstanceOf[UTF8String].toString))

  override def children: Seq[Expression] =
    Seq(contigName, start, end, chainFile, minMatchRatio)

  override def inputTypes = { // scalastyle:ignore
    Seq(StringType, LongType, LongType, StringType, DecimalType)
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes()
    if (!chainFile.foldable) {
      TypeCheckResult.TypeCheckFailure("Chain file must be a constant value")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType =
    StructType(
      Seq(
        StructField("contigName", StringType),
        StructField("start", LongType),
        StructField("end", LongType)))

  override def nullable: Boolean = true

  /**
   * Performs lift over from the input row. The LiftOver API uses a 1-start, fully-closed interval.
   *
   * @param input A row containing a 0-start, half-open interval on the reference sequence.
   * @return A struct containing a 0-start, half-open interval (contigName, start, end) on the query sequence.
   */
  override def eval(input: InternalRow): Any = {
    val _contigName = contigName.eval(input)
    val _start = start.eval(input)
    val _end = end.eval(input)
    val _minMatchRatio = minMatchRatio.eval(input)

    if (_contigName == null || _start == null || _end == null || _minMatchRatio == null) {
      return null
    }

    val inputInterval = new Interval(
      _contigName.asInstanceOf[UTF8String].toString,
      (_start.asInstanceOf[Long] + 1).toInt, // Convert to the 1-based closed-ended Interval start
      _end.asInstanceOf[Long].toInt)

    val outputInterval =
      liftOver.liftOver(inputInterval, _minMatchRatio.asInstanceOf[Decimal].toDouble)
    if (outputInterval == null) {
      return null
    }
    new GenericInternalRow(
      Array(
        UTF8String.fromString(outputInterval.getContig),
        (outputInterval.getStart - 1).toLong, // Convert from the 1-based closed-ended Interval end
        outputInterval.getEnd.toLong
      )
    )
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): LiftOverCoordinatesExpr =
    copy(
      contigName = newChildren(0),
      start = newChildren(1),
      end = newChildren(2),
      chainFile = newChildren(3),
      minMatchRatio = newChildren(4))
}
