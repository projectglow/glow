/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.hls.sql.expressions

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, collect_list, expr}
import org.apache.spark.sql.hls.dsl.expressions._
import org.apache.spark.sql.test.SharedSQLContext

import com.databricks.hls.sql.HLSBaseTest

class FunctionsSuite extends HLSBaseTest {

  test("generate bin id") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val ds = spark.sparkContext
      .parallelize(
        Seq(
          (0L, 5L),
          (3L, 8L)
        )
      )
      .toDF("start", "end")

    val bins = ds.select($"start", bin($"start", $"end", 2))

    val actual = bins
      .groupBy($"start")
      .agg(collect_list($"binId").as("bin"), collect_list($"isLastBin").as("isLastBin"))
      .map {
        case Row(start: Long, bins: Seq[Int], isLastBins: Seq[Boolean]) =>
          (start, bins.zip(isLastBins))
      }
      .collect()
      .sortBy(_._1)

    val expected = Array(
      (0L, Seq((0, false), (1, false), (2, true))),
      (3L, Seq((1, false), (2, false), (3, true)))
    )

    assert(actual sameElements expected)
  }

  test("transform string") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val data = Seq("a", "b", "c", "d")
    val ds = spark.sparkContext.parallelize(data).toDF("id")

    val lowerCase = (x: Char) => x.toLower
    val actual = ds.select(transform($"id", lowerCase)).as[String].collect()
    val expected = data.map(_.toLowerCase)
    assert(actual sameElements expected)
  }

  case class S(s: String)
  def testSplit(name: String, s: String, split: Char, expected: Seq[String]): Unit = test(name) {
    val df = spark.createDataFrame(Seq(S(s)))
    val splits = df
      .select(ascii_char_split(col("s"), split).as("splits"))
      .withColumn("length", expr("size(splits)"))
      .head
    assert(splits.getAs[Int](1) == expected.size)
    assert(splits.getAs[Seq[String]](0) == expected)
  }

  // scalastyle:off nonascii.message
  testSplit("simple", "a,b,c", ',', Seq("a", "b", "c"))
  testSplit("only char", ";", ';', Seq("", ""))
  testSplit("empty string", "", '/', Seq(""))
  testSplit(
    "unicode characters (no split)",
    "☕\uD83E\uDD51\uD83D\uDCA9",
    ',',
    Seq("☕\uD83E\uDD51\uD83D\uDCA9")
  )
  testSplit("unicode characters (split)", "☕,☕", ',', Seq("☕", "☕"))
  // scalastyle:on nonascii.message
}
