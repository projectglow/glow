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

  /**
   * Executes a left outer join with an interval overlap condition accelerated
   * by Databricks' range join optimization <https://docs.databricks.com/en/optimizations/range-join.html>.
   * This function assumes half open intervals i.e., (0, 2) and (1, 2) overlap but (0, 2) and (2, 3) do not.
   */
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
