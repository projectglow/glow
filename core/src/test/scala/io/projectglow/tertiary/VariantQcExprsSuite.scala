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

import scala.util.Random

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions._

import io.projectglow.common.{GenotypeFields, VCFRow}
import io.projectglow.sql.GlowBaseTest

class VariantQcExprsSuite extends GlowBaseTest {

  lazy val testVcf = s"$testDataHome/1kg_sample.vcf"
  lazy private val sess = spark

  private val targetSite = col("contigName") === 1 && col("start") === 904164

  test("missing") {
    val sess = spark
    spark
      .read
      .format("vcf")
      .option("includeSampleIds", true)
      .load(testVcf)
      .filter(targetSite)
      .selectExpr("explode(genotypes)")
      .selectExpr("expand_struct(col)")
      .collect()
  }

  // Golden values are pulled from Hail
  test("hardy weinberg") {
    import sess.implicits._
    val hw = spark
      .read
      .format("vcf")
      .load(testVcf)
      .filter(targetSite)
      .selectExpr("expand_struct(hardy_weinberg(genotypes))")
      .as[HardyWeinbergStats]
      .head
    assert(hw.hetFreqHwe ~== 0.19938860890353427 relTol 0.2)
    assert(hw.pValueHwe ~== 2.8753895001390113e-07 relTol 0.2)
  }

  test("hardy weinberg doesn't crash if there are no homozygous") {
    import sess.implicits._
    spark
      .createDataset(Seq(rowWithCalls(Seq(Seq(0, 1)))))
      .selectExpr("expand_struct(hardy_weinberg(genotypes))")
      .collect() // no crash
  }

  // Golden values are pulled from Hail
  test("call stats") {
    import sess.implicits._
    val stats = spark
      .read
      .format("vcf")
      .load(testVcf)
      .selectExpr("contigName", "start", "expand_struct(call_summary_stats(genotypes))")
      .filter(targetSite)
      .as[CStats]
      .head
    assert(stats.callRate ~== 0.9915493130683899 relTol 0.2)
    assert(stats.nCalled == 704)
    assert(stats.nUncalled == 6)
    assert(stats.nHet == 110)
    assert(stats.nNonRef == 134)
    assert(stats.nAllelesCalled == 1408)
    assert(stats.alleleCounts == Seq(1250, 158))
    assert(stats.alleleFrequencies(0) ~== 0.8877840909090909 relTol 0.2)
    assert(stats.alleleFrequencies(1) ~== 0.11221590909090909 relTol 0.2)
  }

  private def rowWithCalls(calls: Seq[Seq[Int]]): VCFRow = {
    VCFRow(
      "monkey",
      1,
      2,
      Seq.empty,
      "A",
      Seq("G"),
      None,
      Seq.empty,
      Map.empty,
      calls.map { call =>
        GenotypeFields(
          None,
          None,
          Some(call.map(_.toShort)),
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          Map.empty
        )
      }
    )
  }

  test("call stats (missing genotypes)") {
    import sess.implicits._
    val df = spark.createDataset(Seq(rowWithCalls(Seq(Seq()))))
    val stats = df
      .selectExpr("expand_struct(call_summary_stats(genotypes))")
      .as[CStats]
      .head
    val expected = CStats(0, 0, 1, 0, Seq.empty, 0, 0, Seq.empty, Seq.empty)
    assert(stats == expected)
  }

  test("call stats (haploid)") {
    import sess.implicits._
    val stats = spark
      .createDataset(Seq(rowWithCalls(Seq(Seq(0)))))
      .selectExpr("expand_struct(call_summary_stats(genotypes))")
      .as[CStats]
      .head
    val expected = CStats(1, 1, 0, 0, Seq(1), 0, 1, Seq(1), Seq(1))
    assert(stats == expected)
  }

  test("call stats (weird genotype struct)") {
    import sess.implicits._
    val df = spark.createDataFrame(Seq(rowWithCalls(Seq(Seq(0, 1)))))
    val transformed = df.selectExpr(
      "transform(genotypes, g -> struct(g.depth as depth, g.calls as calls)) as newG"
    )
    val stats = transformed
      .selectExpr("expand_struct(call_summary_stats(newG))")
      .as[CStats]
      .head
    assert(stats.nCalled == 1)
  }

  test("array stats") {
    import sess.implicits._
    val df = spark.createDataFrame(Seq(Datum(Array(0, 1, 2))))
    val stats = df
      .selectExpr("expand_struct(array_summary_stats(numbers))")
      .as[ArraySummaryStats]
      .head

    assert(stats.min.get == 0)
    assert(stats.max.get == 2)
    assert(stats.mean.get == 1)
    assert(stats.stdDev.get == 1)
  }

  test("array stats (empty)") {
    import sess.implicits._
    val df = spark.createDataFrame(Seq(Datum(Array.empty)))
    val stats = df
      .selectExpr("expand_struct(array_summary_stats(numbers))")
      .as[ArraySummaryStats]
      .head
    val expected = ArraySummaryStats(None, None, None, None)
    assert(stats == expected)
  }

  test("array stats (1 element)") {
    import sess.implicits._
    val df = spark.createDataFrame(Seq(Datum(Array(1))))
    val stats = df
      .selectExpr("expand_struct(array_summary_stats(numbers))")
      .as[ArraySummaryStats]
      .head
    assert(stats.mean.get == 1)
    assert(stats.stdDev.get.isNaN)
    assert(stats.min.get == 1)
    assert(stats.max.get == 1)
  }

  test("array stats (contains null)") {
    import sess.implicits._
    val df = spark.range(1).withColumn("numbers", array(lit(1d), lit(3d)))
    val stats = df
      .selectExpr("expand_struct(array_summary_stats(numbers))")
      .as[ArraySummaryStats]
      .head
    assert(stats == ArraySummaryStats(Some(2), Some(Math.sqrt(2)), Some(1), Some(3)))
  }

  test("array stats (negative values)") {
    import sess.implicits._
    val df = spark.range(1).withColumn("numbers", array(lit(-1d), lit(-3d)))
    val stats = df
      .selectExpr("expand_struct(array_summary_stats(numbers))")
      .as[ArraySummaryStats]
      .head
    assert(stats == ArraySummaryStats(Some(-2), Some(Math.sqrt(2)), Some(-3), Some(-1)))
  }

  // Golden values are pulled from Hail
  test("dp stats") {
    import sess.implicits._
    val stats = spark
      .read
      .format("vcf")
      .load(testVcf)
      .filter(targetSite)
      .selectExpr("expand_struct(dp_summary_stats(genotypes))")
      .as[ArraySummaryStats]
      .head
    assert(stats.mean.get ~== 7.019886363636361 relTol 0.2)
    assert(stats.stdDev.get ~== 3.9050742032055332 relTol 0.2)
    assert(stats.min.get == 1)
    assert(stats.max.get == 23)
  }

  // Golden values are pulled from Hail
  test("gq stats") {
    import sess.implicits._
    val stats = spark
      .read
      .format("vcf")
      .load(testVcf)
      .filter(targetSite)
      .selectExpr("expand_struct(gq_summary_stats(genotypes))")
      .as[ArraySummaryStats]
      .head
    assert(stats.mean.get ~== 26.856534090909086 relTol 0.2)
    assert(stats.stdDev.get ~== 22.18115337984482 relTol 0.2)
    assert(stats.min.get == 2)
    assert(stats.max.get == 99)
  }

  test("write to parquet") {
    val tmpFile = s"/tmp/${Random.alphanumeric.take(10).mkString}"
    spark
      .read
      .format("vcf")
      .load(testVcf)
      .withColumn("stats", expr("call_summary_stats(genotypes)"))
      .withColumn("hw", expr("hardy_weinberg(genotypes)"))
      .write
      .format("parquet")
      .save(tmpFile)
  }

  test("analysis error when genotype doesn't exist for call stats") {
    val e = intercept[AnalysisException] {
      spark
        .createDataFrame(Seq(Datum(Array(1))))
        .selectExpr("call_summary_stats(numbers)")
        .collect()
    }
    assert(e.getMessage.contains("Genotypes field must be an array of structs"))
  }

  test("analysis error when genotype doesn't exist for hardy weinberg") {
    val e = intercept[AnalysisException] {
      spark
        .createDataFrame(Seq(Datum(Array(1))))
        .selectExpr("hardy_weinberg(numbers)")
        .collect()
    }
    assert(e.getMessage.contains("Genotypes field must be an array of structs"))
  }

  test("analysis error when genotype is missing calls for call stats") {
    val e = intercept[AnalysisException] {
      spark
        .read
        .format("vcf")
        .load(testVcf)
        .selectExpr("transform(genotypes, gt -> subset_struct(gt, 'sampleId')) as callFreeGts")
        .selectExpr("call_summary_stats(callFreeGts)")
        .collect()
    }
    assert(
      e.getMessage
        .contains(
          "Genotype struct was missing required fields: (name: calls, type: ArrayType(IntegerType,true))"))
  }

}

case class ArraySummaryStats(
    mean: Option[Double],
    stdDev: Option[Double],
    min: Option[Double],
    max: Option[Double])
case class Datum(numbers: Array[Double])
case class HardyWeinbergStats(hetFreqHwe: Double, pValueHwe: Double)
case class CStats(
    callRate: Double,
    nCalled: Int,
    nUncalled: Int,
    nHet: Int,
    nHomozygous: Seq[Int],
    nNonRef: Int,
    nAllelesCalled: Int,
    alleleCounts: Seq[Int],
    alleleFrequencies: Seq[Double])
