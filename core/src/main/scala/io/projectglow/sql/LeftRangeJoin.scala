package io.projectglow.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object LeftRangeJoin {
  def join(
      left: DataFrame,
      right: DataFrame,
      leftStart: Column,
      rightStart: Column,
      leftEnd: Column,
      rightEnd: Column,
      extraJoinExpr: Column = lit(true),
      binSize: Int = 5000): DataFrame = {
    val rightHinted = right.hint("range_join", binSize)
    val rangeExpr = leftStart < rightEnd && rightStart < leftEnd
    val leftPoints = left.where(leftEnd - leftStart === 1)
    val leftIntervals = left.where(leftEnd - leftStart =!= 1)
    val pointsJoined = leftPoints.join(
      rightHinted,
      leftStart >= rightStart && leftStart < rightEnd && extraJoinExpr,
      joinType = "left")
    val longVarsInner = leftIntervals.join(rightHinted, rangeExpr && extraJoinExpr)
    leftIntervals.join(longVarsInner, leftIntervals.columns, joinType = "left").union(pointsJoined)
  }
}
