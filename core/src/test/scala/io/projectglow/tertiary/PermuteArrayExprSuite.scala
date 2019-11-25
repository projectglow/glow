package io.projectglow.tertiary

import io.projectglow.sql.GlowBaseTest
import org.apache.spark.sql.functions.{lit, when}

class PermuteArrayExprSuite extends GlowBaseTest{

  private lazy val sess = spark

  test("basic test") {
    val dataArray = Array(5, 6, 7, 8)
    val indexArray = Array(1, 2)
    val aRow = spark.range(1).select(lit(dataArray) as "dataArray", lit(indexArray) as "indexArray")
    val permutedData = aRow.selectExpr("permute_array(dataArray,indexArray) as permutedData")
      .take(1)
      .map(r => r.getAs[Seq[Int]]("permutedData")).head
    assert(permutedData == indexArray.map(i => dataArray(i-1)).toSeq)
  }

  test(testName = "null indices") {
    import sess.implicits._
    val dataArray = Array(5, 6, 7, 8)
    val aRow = spark.range(1).select(lit(dataArray) as "dataArray", lit(null).cast("array<int>") as "indexArray")
    val nullCheck = aRow.selectExpr("permute_array(dataArray,indexArray) as permutedData")
      .withColumn("nullCheck", when($"permutedData".isNull, lit(true)).otherwise(lit(false)))
      .take(1)
      .map(r => r.getAs[Boolean]("nullCheck")).head
    assert(nullCheck)
  }

  test(testName = "null data") {
    import sess.implicits._
    val indexArray = Array(3, 2)
    val aRow = spark.range(1).select(lit(null).cast("array<double>") as "dataArray", lit(indexArray) as "indexArray")
    val nullCheck = aRow.selectExpr("permute_array(dataArray,indexArray) as permutedData")
      .withColumn("nullCheck", when($"permutedData".isNull, lit(true)).otherwise(lit(false)))
      .take(1)
      .map(r => r.getAs[Boolean]("nullCheck")).head
    assert(nullCheck)
  }

  test(testName = "index too large") {
    import sess.implicits._
    val dataArray = Array(5, 6, 7, 8)
    val indexArray = Array(2, 3, 4, 5)
    val aRow = spark.range(1).select(lit(dataArray) as "dataArray", lit(indexArray) as "indexArray")
    val nullCheck = aRow.selectExpr("permute_array(dataArray,indexArray) as permutedData")
      .withColumn("nullCheck", when($"permutedData".isNull, lit(true)).otherwise(lit(false)))
      .take(1)
      .map(r => r.getAs[Boolean]("nullCheck")).head
    assert(nullCheck)
  }

  test(testName = "index too small") {
    import sess.implicits._
    val dataArray = Array(5, 6, 7, 8)
    val indexArray = Array(0, 1, 2, 3)
    val aRow = spark.range(1).select(lit(dataArray) as "dataArray", lit(indexArray) as "indexArray")
    val nullCheck = aRow.selectExpr("permute_array(dataArray,indexArray) as permutedData")
      .withColumn("nullCheck", when($"permutedData".isNull, lit(true)).otherwise(lit(false)))
      .take(1)
      .map(r => r.getAs[Boolean]("nullCheck")).head
    assert(nullCheck)
  }

  test(testName = "empty indices") {
    val dataArray = Array(5, 6, 7, 8)
    val indexArray = Array.empty[Int]
    val aRow = spark.range(1).select(lit(dataArray) as "dataArray", lit(indexArray) as "indexArray")
    val permutedData = aRow.selectExpr("permute_array(dataArray,indexArray) as permutedData")
      .take(1)
      .map(r => r.getAs[Seq[Int]]("permutedData")).head
    assert(permutedData.isEmpty)
  }

  test(testName = "empty data") {
    import sess.implicits._
    val dataArray = Array.empty[String]
    val indexArray = Array(0, 1, 2, 3, 4)
    val aRow = spark.range(1).select(lit(dataArray) as "dataArray", lit(indexArray) as "indexArray")
    val nullCheck = aRow.selectExpr("permute_array(dataArray,indexArray) as permutedData")
      .withColumn("nullCheck", when($"permutedData".isNull, lit(true)).otherwise(lit(false)))
      .take(1)
      .map(r => r.getAs[Boolean]("nullCheck")).head
    assert(nullCheck)
  }

}