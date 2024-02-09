package io.projectglow.sql

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

class LeftRangeJoinSuite extends GlowBaseTest {
  case class Interval(start: Long, end: Long)
  case class StringWithInterval(name: String, start: Long, end: Long)
  private lazy val sess = spark
  private def naiveJoin(
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
      joinType = "left")
  }

  private def compareToNaive(
      left: DataFrame,
      right: DataFrame,
      extraJoinExpr: Column = lit(true)): Unit = {
    val (leftStart, rightStart) = (left("start"), right("start"))
    val (leftEnd, rightEnd) = (left("end"), right("end"))
    val glow =
      LeftRangeJoin.join(left, right, leftStart, rightStart, leftEnd, rightEnd, extraJoinExpr)
    val naive = naiveJoin(left, right, leftStart, rightStart, leftEnd, rightEnd, extraJoinExpr)
    assert(glow.count() == naive.count() && glow.except(naive).count() == 0)
  }

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
    val joined = naiveJoin(
      left,
      right,
      left("start"),
      right("start"),
      left("end"),
      right("end"),
      left("name") === right("name"))
    assert(joined.count() == 5) // All five left rows are present
    assert(joined.where(right("start").isNull).count() == 2) // Unmatched left rows have no right fields
    // Unmatched right rows are not present
    assert(
      joined
        .where(right("name") === "c" && right("start") === 2 && right("end") === 5)
        .count() == 0)
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

  test("Fully contained ranges") {
    val left = spark.createDataFrame(Seq(Interval(0, 10)))
    val right = spark.createDataFrame(Seq(Interval(2, 5)))

    compareToNaive(left, right)
  }

  test("Identical start and end points") {
    val left = spark.createDataFrame(Seq(Interval(0, 10)))
    val right = spark.createDataFrame(Seq(Interval(2, 5)))
    compareToNaive(left, right)
  }
}
