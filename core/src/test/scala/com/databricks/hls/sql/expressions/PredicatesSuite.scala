package com.databricks.hls.sql.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.ExpressionEvalHelper
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hls.dsl.expressions.overlaps

class PredicatesSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("overlaps") {
    val col = overlaps(lit(0), lit(3), lit(2), lit(5))
    val expected = true
    checkEvaluation(col.expr, expected)
  }

  test("no overlap") {
    val col = overlaps(lit(0), lit(2), lit(2), lit(5))
    checkEvaluation(col.expr, false)
  }
}
