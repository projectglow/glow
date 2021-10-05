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

package io.projectglow.tertiary

import org.apache.spark.sql.functions._
import io.projectglow.sql.GlowBaseTest
import org.apache.spark.sql.AnalysisException

class AggregateByIndexSuite extends GlowBaseTest {
  private lazy val sess = spark

  test("basic") {
    import sess.implicits._
    val results = spark
      .createDataFrame(Seq(Tuple1(Seq(1L, 2L, 3L))))
      .selectExpr("aggregate_by_index(_1, 0l, (x, y) -> x + y, (x, y) -> x + y, x -> x) as agg")
      .as[Seq[Long]]
      .head
    assert(results == Seq(1L, 2L, 3L))
  }

  test("basic (scala API)") {
    import io.projectglow.functions._
    import org.apache.spark.sql.functions._
    import sess.implicits._
    val results = spark
      .createDataFrame(Seq(Tuple1(Seq(1L, 2L, 3L))))
      .select(aggregate_by_index($"_1", lit(0L), (x, y) => x + y, (x, y) => x + y, x => x))
      .as[Seq[Long]]
      .head
    assert(results == Seq(1L, 2L, 3L))
  }

  test("with types changes") {
    import sess.implicits._
    val results = spark
      .createDataFrame(Seq(Tuple1(Seq(1L, 2L, 3L))))
      .selectExpr("""
          |aggregate_by_index(
          |_1,
          |0d,
          |(x, y) -> cast(x as double) + cast(y as double),
          |(x, y) -> x + y,
          |x -> cast(x as string))
        """.stripMargin)
      .as[Seq[String]]
      .head
    assert(results == Seq("1.0", "2.0", "3.0"))
  }

  test("with grouping") {
    import sess.implicits._
    val df = spark.createDataFrame(
      Seq(
        ("a", Seq(1, 2, 3)),
        ("b", Seq(4, 5, 6))
      ))
    val results = df
      .groupBy("_1")
      .agg(expr("aggregate_by_index(_2, 0, (x, y) -> x + y, (x, y) -> x + y, x -> x)"))
      .orderBy("_1")
      .as[(String, Seq[Int])]
      .collect
      .toSeq
    assert(results == Seq(("a", Seq(1, 2, 3)), ("b", Seq(4, 5, 6))))
  }

  test("mean function") {
    import sess.implicits._

    val result = spark
      .range(10000)
      .withColumn("values", array_repeat(col("id"), 100))
      .selectExpr(
        """
          |aggregate_by_index(
          |values,
          |struct(0l, 0l),
          |(state, el) -> struct(state.col1 + 1, state.col2 + el),
          |(state1, state2) -> struct(state1.col1 + state2.col1, state1.col2 + state2.col2),
          |state -> if(state.col1 = 0, null, state.col2 / state.col1))
        """.stripMargin
      )
      .as[Seq[Double]]
      .head
    assert(result == Seq.fill(100)(4999.5d))

  }

  test("type error") {
    intercept[AnalysisException] {
      spark
        .range(10000)
        .withColumn("values", array_repeat(col("id"), 100))
        .selectExpr("""
            |aggregate_by_index(
            |values,
            |1,
            |(acc, el) -> size(acc), -- type error since size expects a map or array
            |(acc1, acc2) -> acc1 + acc2)
          """.stripMargin)
        .collect()
    }
  }

  test("no evaluate function") {
    import sess.implicits._
    val results = spark
      .range(10)
      .withColumn("values", expr("transform(array_repeat(0, 5), (el, idx) -> idx)"))
      .selectExpr("""
          |aggregate_by_index(
          |values,
          |0,
          |(acc, el) -> acc + el,
          |(acc1, acc2) -> acc1 + acc2)
        """.stripMargin)
      .as[Seq[Int]]
      .collect
      .toSeq
    results.foreach { row =>
      assert(row == Seq(0, 10, 20, 30, 40))
    }
  }

  test("empty dataframe") {
    import sess.implicits._
    val result = spark
      .emptyDataset[Tuple1[Seq[Int]]]
      .selectExpr("""
          |aggregate_by_index(
          |_1,
          |0,
          |(acc, el) -> acc + el,
          |(acc1, acc2) -> acc1 + acc2)
        """.stripMargin)
      .as[Seq[Int]]
      .head
    if (spark.version >= "3.0") {
      assert(result == null)
    } else {
      assert(result == Seq.empty)
    }
  }

  test("null array") {
    import sess.implicits._
    val result = spark
      .createDataFrame(Seq(Tuple1(null), Tuple1(Seq(1))))
      .selectExpr("""
          |aggregate_by_index(
          |_1,
          |0,
          |(acc, el) -> acc + el,
          |(acc1, acc2) -> acc1 + acc2)
        """.stripMargin)
      .as[Seq[Option[Int]]]
      .head
    assert(result == Seq(None))
  }
}
