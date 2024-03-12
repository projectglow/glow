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

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

abstract class OverlapJoinSuite extends GlowBaseTest {
  case class Interval(start: Long, end: Long)
  case class StringWithInterval(name: String, start: Long, end: Long)
  protected lazy val sess = spark
  protected def isSemi: Boolean
  protected def naiveLeftJoin(
      left: DataFrame,
      right: DataFrame,
      leftStart: Column,
      rightStart: Column,
      leftEnd: Column,
      rightEnd: Column,
      extraJoinExpr: Column = lit(true)): DataFrame = {
    left.join(
      right,
      leftStart < rightEnd && rightStart < leftEnd && extraJoinExpr,
      joinType = if (isSemi) "left_semi" else "left")
  }

  private def compareToNaive(
      left: DataFrame,
      right: DataFrame,
      extraJoinExpr: Column = lit(true)): Unit = {
    val (leftStart, rightStart) = (left("start"), right("start"))
    val (leftEnd, rightEnd) = (left("end"), right("end"))
    val glow = if (isSemi) {
      LeftOverlapJoin.leftSemiJoin(
        left,
        right,
        leftStart,
        rightStart,
        leftEnd,
        rightEnd,
        extraJoinExpr)
    } else {
      LeftOverlapJoin.leftJoin(left, right, leftStart, rightStart, leftEnd, rightEnd, extraJoinExpr)
    }
    val naive = naiveLeftJoin(left, right, leftStart, rightStart, leftEnd, rightEnd, extraJoinExpr)
    assert(glow.count() == naive.count() && glow.except(naive).count() == 0)
  }

  test("Simple long intervals") {
    val left = spark.createDataFrame(
      Seq(
        Interval(1, 7),
        Interval(3, 10)
      ))
    val right = spark.createDataFrame(
      Seq(
        Interval(1, 4),
        Interval(2, 5)
      ))
    compareToNaive(left, right)
  }

  test("Unmatched left intervals") {
    val left = spark.createDataFrame(
      Seq(
        Interval(1, 3),
        Interval(7, 10)
      ))
    val right = spark.createDataFrame(
      Seq(
        Interval(2, 5)
      ))
    compareToNaive(left, right)
  }

  test("Unmatched right intervals") {
    val left = spark.createDataFrame(
      Seq(
        Interval(2, 5)
      ))
    val right = spark.createDataFrame(
      Seq(
        Interval(1, 3),
        Interval(7, 10)
      ))
    compareToNaive(left, right)
  }

  test("Points and intervals") {
    val left = spark.createDataFrame(
      Seq(
        Interval(2, 3),
        Interval(1, 3),
        Interval(7, 10),
        Interval(8, 9)
      ))
    val right = spark.createDataFrame(
      Seq(
        Interval(2, 5)
      ))
    compareToNaive(left, right)
  }

  test("extraJoinExpr") {
    val left = spark.createDataFrame(
      Seq(
        StringWithInterval("a", 1, 7),
        StringWithInterval("a", 3, 10),
        StringWithInterval("c", 2, 3)
      ))
    val right = spark.createDataFrame(
      Seq(
        StringWithInterval("a", 1, 4),
        StringWithInterval("b", 2, 5)
      ))
    compareToNaive(left, right, left("name") === right("name"))
  }

  test("table aliases") {
    val left = spark.createDataFrame(
      Seq(
        StringWithInterval("a", 1, 7),
        StringWithInterval("a", 3, 10),
        StringWithInterval("c", 2, 3)
      ))
    val right = spark.createDataFrame(
      Seq(
        StringWithInterval("a", 1, 4),
        StringWithInterval("b", 2, 5)
      ))
    compareToNaive(left.alias("left"), right.alias("right"))
  }

  test("handles negative intervals") {
    val left = spark.createDataFrame(
      Seq(
        StringWithInterval("b", 2, 7),
        StringWithInterval("b", 3, 2)
      ))
    val right = spark.createDataFrame(
      Seq(
        StringWithInterval("a", 3, 4)
      ))
    compareToNaive(left, right)
  }

  test("Ranges that touch at a point should not join") {
    val left = spark.createDataFrame(Seq(Interval(0, 10)))
    val right = spark.createDataFrame(Seq(Interval(10, 20)))
    compareToNaive(left, right)
  }

  test("Ranges that touch at a point should not join (point)") {
    val left = spark.createDataFrame(Seq(Interval(9, 10)))
    val right = spark.createDataFrame(Seq(Interval(10, 20)))
    compareToNaive(left, right)
  }

  test("Fully contained ranges") {
    val left = spark.createDataFrame(Seq(Interval(0, 10)))
    val right = spark.createDataFrame(Seq(Interval(2, 5)))

    compareToNaive(left, right)
  }

  test("Identical start and end points") {
    val left = spark.createDataFrame(Seq(Interval(0, 10)))
    val right = spark.createDataFrame(Seq(Interval(0, 10)))
    compareToNaive(left, right)
  }

  test("Identical start points (point)") {
    val left = spark.createDataFrame(Seq(Interval(0, 1)))
    val right = spark.createDataFrame(Seq(Interval(0, 10)))
    compareToNaive(left, right)
  }
}

class LeftOverlapJoinSuite extends OverlapJoinSuite {
  override def isSemi: Boolean = false
  test("naive implementation") {
    import sess.implicits._
    val left = Seq(
      ("a", 1, 10), // matched
      ("a", 2, 3), // matched
      ("a", 5, 7), // unmatched
      ("a", 2, 5), // matched
      ("b", 1, 10) // unmatched
    ).toDF("name", "start", "end")
    val right = Seq(
      ("a", 2, 5), // matched
      ("c", 2, 5) // unmatched
    ).toDF("name", "start", "end")
    val joined = naiveLeftJoin(
      left,
      right,
      left("start"),
      right("start"),
      left("end"),
      right("end"),
      left("name") === right("name"))
    assert(joined.count() == 5) // All five left rows are present
    assert(
      joined.where(right("start").isNull).count() == 2
    ) // Unmatched left rows have no right fields
    // Unmatched right rows are not present
    assert(
      joined
        .where(right("name") === "c" && right("start") === 2 && right("end") === 5)
        .count() == 0)
  }

  test("duplicate column names (prefix)") {
    val left = spark
      .createDataFrame(
        Seq(
          StringWithInterval("a", 1, 7),
          StringWithInterval("a", 3, 10),
          StringWithInterval("c", 2, 3)
        ))
      .alias("left")
    val right = spark
      .createDataFrame(
        Seq(
          StringWithInterval("a", 1, 4),
          StringWithInterval("b", 2, 5)
        ))
      .alias("right")
    val joined = LeftOverlapJoin.leftJoin(
      left,
      right,
      left("start"),
      right("start"),
      left("end"),
      right("end"),
      rightPrefix = Some("right_"))
    right.columns.foreach { c =>
      assert(joined.columns.contains(s"right_$c"))
    }
    withTempDir { f =>
      val tablePath = f.toPath.resolve("joined")
      joined.write.parquet(tablePath.toString)
      spark.read.parquet(tablePath.toString).collect() // Can read and write table
    }
  }
}

class LeftSemiOverlapJoinSuite extends OverlapJoinSuite {
  override def isSemi: Boolean = true
  test("naive implementation (semi)") {
    import sess.implicits._
    val left = Seq(
      ("a", 1, 10), // matched
      ("a", 2, 3), // matched
      ("a", 5, 7), // unmatched
      ("a", 2, 5), // matched
      ("b", 1, 10) // unmatched
    ).toDF("name", "start", "end")
    val right = Seq(
      ("a", 2, 5), // matched
      ("c", 2, 5) // unmatched
    ).toDF("name", "start", "end")
    val joined = naiveLeftJoin(
      left,
      right,
      left("start"),
      right("start"),
      left("end"),
      right("end"),
      left("name") === right("name"))
    assert(
      joined.as[(String, Int, Int)].collect().toSet == Set(("a", 1, 10), ("a", 2, 3), ("a", 2, 5)))
  }
}
