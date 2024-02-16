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

package io.projectglow.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object LeftOverlapJoin {

  private def maybePrefixRightColumns(
      table: DataFrame,
      leftColumns: Seq[Column],
      rightColumns: Seq[Column],
      prefixOpt: Option[String]): DataFrame = prefixOpt match {
    case Some(prefix) =>
      val renamedRightCols = rightColumns.map(c => c.alias(s"${prefix}$c"))
      table.select((leftColumns ++ renamedRightCols): _*)
    case None => table
  }

  /**
   * Executes a left outer join with an interval overlap condition accelerated
   * by Databricks' range join optimization <https://docs.databricks.com/en/optimizations/range-join.html>.
   * This function assumes half open intervals i.e., (0, 2) and (1, 2) overlap but (0, 2) and (2, 3) do not.
   *
   * @param extraJoinExpr If provided, this expression will be included in the join criteria
   * @param rightPrefix If provided, all columns from the right table will begin have their names prefixed with
   *                    this value in the joined table
   * @param binSize The bin size for the range join optimization. Consult the Databricks documentation for more info.
   */
  def leftJoin(
      left: DataFrame,
      right: DataFrame,
      leftStart: Column,
      rightStart: Column,
      leftEnd: Column,
      rightEnd: Column,
      extraJoinExpr: Column = lit(true),
      rightPrefix: Option[String] = None,
      binSize: Int = 5000): DataFrame = {
    val rightPrepared = right.hint("range_join", binSize)
    val rangeExpr = leftStart < rightEnd && rightStart < leftEnd
    val leftPoints = left.where(leftEnd - leftStart === 1)
    val leftIntervals = left.where(leftEnd - leftStart =!= 1)
    val pointsJoined = leftPoints.join(
      rightPrepared,
      leftStart >= rightStart && leftStart < rightEnd && extraJoinExpr,
      joinType = "left")
    val longVarsInner = leftIntervals.join(rightPrepared, rangeExpr && extraJoinExpr)
    val result = leftIntervals
      .join(longVarsInner, leftIntervals.columns, joinType = "left")
      .union(pointsJoined)
    maybePrefixRightColumns(
      result,
      left.columns.map(left.apply),
      rightPrepared.columns.map(rightPrepared.apply),
      rightPrefix)
  }

  /**
   * Executes a left semi join with an interval overlap condition accelerated
   * by Databricks' range join optimization <https://docs.databricks.com/en/optimizations/range-join.html>.
   * This function assumes half open intervals i.e., (0, 2) and (1, 2) overlap but (0, 2) and (2, 3) do not.
   *
   * @param extraJoinExpr If provided, this expression will be included in the join criteria
   * @param binSize The bin size for the range join optimization. Consult the Databricks documentation for more info.
   */
  def leftSemiJoin(
      left: DataFrame,
      right: DataFrame,
      leftStart: Column,
      rightStart: Column,
      leftEnd: Column,
      rightEnd: Column,
      extraJoinExpr: Column = lit(true),
      binSize: Int = 5000): DataFrame = {
    val rightPrepared = right.hint("range_join", binSize)
    val rangeExpr = leftStart < rightEnd && rightStart < leftEnd
    val leftPoints = left.where(leftEnd - leftStart === 1)
    val leftIntervals = left.where(leftEnd - leftStart =!= 1)
    val pointsJoined = leftPoints.join(
      rightPrepared,
      leftStart >= rightStart && leftStart < rightEnd && extraJoinExpr,
      joinType = "left_semi")
    val longVarsInner = leftIntervals.join(rightPrepared, rangeExpr && extraJoinExpr)
    val longVarsLeftSemi =
      longVarsInner.select(leftIntervals.columns.map(c => leftIntervals(c)): _*).dropDuplicates()
    longVarsLeftSemi.union(pointsJoined)
  }
}
