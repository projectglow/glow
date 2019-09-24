package com.databricks.hls.tertiary

import java.io.File

import htsjdk.samtools.liftover.LiftOver
import htsjdk.samtools.util.Interval
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, ImplicitCastInputTypes}
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
 * @param minMatchRatioOpt The minimum fraction of bases that must remap to lift over successfully.
 */
case class LiftOverCoordinatesExpr(
    contigName: Expression,
    start: Expression,
    end: Expression,
    chainFile: Expression,
    minMatchRatioOpt: Option[Expression])
    extends CodegenFallback
    with ImplicitCastInputTypes {

  private lazy val liftOver = new LiftOver(
    new File(chainFile.eval().asInstanceOf[UTF8String].toString))

  override def children: Seq[Expression] =
    Seq(contigName, start, end, chainFile) ++ minMatchRatioOpt
  override def inputTypes = { // scalastyle:ignore
    Seq(StringType, LongType, LongType, StringType) ++ minMatchRatioOpt.map(_ => DecimalType)
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
    val _minMatchRatio = minMatchRatioOpt.map(_.eval(input))

    if (_contigName == null || _start == null || _end == null || _minMatchRatio.contains(null)) {
      return null
    }

    val inputInterval = new Interval(
      _contigName.asInstanceOf[UTF8String].toString,
      (_start.asInstanceOf[Long] + 1).toInt, // Convert to the 1-based closed-ended Interval start
      _end.asInstanceOf[Long].toInt)
    val minMatchRatio = _minMatchRatio.map(_.asInstanceOf[Decimal].toDouble).getOrElse(0.95)

    val outputInterval = liftOver.liftOver(inputInterval, minMatchRatio)
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
}
