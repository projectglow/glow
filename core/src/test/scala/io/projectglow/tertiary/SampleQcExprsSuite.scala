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

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.functions.expr

import io.projectglow.common.{VCFRow, VariantSchemas}
import io.projectglow.sql.GlowBaseTest

class SampleQcExprsSuite extends GlowBaseTest {
  lazy val testVcf = s"$testDataHome/1000G.phase3.broad.withGenotypes.chr20.10100000.vcf"
  lazy val na12878 = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy private val sess = spark

  gridTest("sample_call_summary_stats high level test")(Seq(true, false)) { sampleIds =>
    import sess.implicits._
    val df = readVcf(na12878, sampleIds)
    val stats = df
      .selectExpr("sample_call_summary_stats(genotypes, referenceAllele, alternateAlleles) as qc")
      .selectExpr("expand_struct(qc[0])")
      .as[TestSampleCallStats]
      .head

    // Golden value is from Hail 0.2.12
    val expected = TestSampleCallStats(
      1,
      1075,
      0,
      1,
      893,
      181,
      1082,
      85,
      98,
      659,
      423,
      2,
      659.toDouble / 423,
      85.toDouble / 98,
      893.toDouble / 181
    )
    assert(stats == expected)
  }

  case class GenotypeFields(calls: Array[Int], sampleId: String, depth: Option[Int])
  case class SimpleVariant(
      referenceAllele: String,
      alternateAlleles: Seq[String],
      genotypes: Seq[GenotypeFields])

  private def makeDf(ref: String, alts: Seq[String], calls: Array[Int]): DataFrame = {
    val gf = GenotypeFields(calls, "monkey", None)
    spark.createDataFrame(Seq(SimpleVariant(ref, alts, Seq(gf))))
  }

  private def makeDf(dp: Seq[Option[Int]]): DataFrame = {
    val data = dp.map { d =>
      val genotype = GenotypeFields(null, "monkey", d)
      SimpleVariant("A", Seq("T"), Seq(genotype))
    }
    spark.createDataFrame(data)
  }

  private def readVcf(path: String, includeSampleIds: Boolean = true): DataFrame = {
    spark
      .read
      .format("vcf")
      .option("includeSampleIds", includeSampleIds)
      .load(path)
      .repartition(4)
  }

  private def toCallStats(df: DataFrame): Seq[TestSampleCallStats] = {
    import sess.implicits._
    df.selectExpr("sample_call_summary_stats(genotypes, referenceAllele, alternateAlleles) as qc")
      .selectExpr("explode(qc) as sqc")
      .selectExpr("expand_struct(sqc)")
      .as[TestSampleCallStats]
      .collect()
  }

  test("heterozygous only") {
    val stats = toCallStats(makeDf("A", Seq("G"), Array(0, 1)))
    assert(stats.head.nHet == 1)
    assert(stats.head.nTransition == 1)
    assert(stats.head.nTransversion == 0)
    assert(stats.head.rHetHomVar == Double.PositiveInfinity)
  }

  test("empty dataframe") {
    val stats = toCallStats(sess.createDataFrame(sess.sparkContext.emptyRDD[Row], VCFRow.schema))
    assert(stats.isEmpty) // No error expected
  }

  gridTest("dp stats")(Seq(true, false)) { sampleIds =>
    import sess.implicits._
    val stats = readVcf(na12878, includeSampleIds = sampleIds)
      .selectExpr("sample_dp_summary_stats(genotypes) as stats")
      .selectExpr("explode(stats) as dp_stats")
      .selectExpr("expand_struct(dp_stats)")
      .as[ArraySummaryStats]
      .head
    assert(stats.min.get ~== 1 relTol 0.2)
    assert(stats.max.get ~== 645 relTol 0.2)
    assert(stats.mean.get ~== 72.2 relTol 0.2)
    assert(stats.stdDev.get ~== 75.6 relTol 0.2)
  }

  test("dp stats (with null)") {
    import sess.implicits._
    val stats = makeDf(Seq(Some(1), None, Some(3)))
      .selectExpr("sample_dp_summary_stats(genotypes) as stats")
      .selectExpr("explode(stats) as stats")
      .selectExpr("expand_struct(stats)")
      .as[ArraySummaryStats]
      .head()

    val expected = ArraySummaryStats(Some(2), Some(Math.sqrt(2)), Some(1), Some(3))
    assert(stats == expected)
  }

  test("gq stats") {
    import sess.implicits._
    val stats = readVcf(na12878)
      .selectExpr("sample_gq_summary_stats(genotypes) as stats")
      .selectExpr("explode(stats) as stats")
      .selectExpr("expand_struct(stats)")
      .as[ArraySummaryStats]
      .head
    assert(stats.min.get ~== 3 relTol 0.2)
    assert(stats.max.get ~== 99 relTol 0.2)
    assert(stats.mean.get ~== 89.2 relTol 0.2)
    assert(stats.stdDev.get ~== 23.2 relTol 0.2)
  }

  private val expressionsToTest = Seq(
    "expand_struct(sample_call_summary_stats(genotypes, referenceAllele, alternateAlleles)[0])",
    "expand_struct(sample_gq_summary_stats(genotypes)[0])",
    "expand_struct(sample_dp_summary_stats(genotypes)[0])"
  )
  private val testCases = expressionsToTest
    .flatMap(expr => Seq((expr, true), (expr, false)))
  gridTest("sample ids are propagated if included")(testCases) {
    case (expr, sampleIds) =>
      import sess.implicits._
      val df = readVcf(na12878, sampleIds)
        .selectExpr(expr)
      val outputSchema = df.schema
      assert(outputSchema.exists(_.name == VariantSchemas.sampleIdField.name) == sampleIds)
      if (sampleIds) {
        assert(df.select("sampleId").as[String].head == "NA12878")
      }
  }

  private val typeTransformations = Seq(
    ("genotypes", "referenceAllele", "Genotypes field must be an array of structs"),
    (
      "genotypes",
      "transform(genotypes, gt -> subset_struct(gt, 'sampleId'))",
      "Genotype struct was missing required fields"),
    ("referenceAllele", "alternateAlleles", "Reference allele must be a string"),
    ("alternateAlleles", "referenceAllele", "Alternate alleles must be an array of strings")
  )
  gridTest("type check failures")(typeTransformations) {
    case (colName, colExpr, error) =>
      val e = intercept[AnalysisException] {
        readVcf(na12878)
          .withColumn(colName, expr(colExpr))
          .selectExpr("sample_call_summary_stats(genotypes, referenceAllele, alternateAlleles)")
          .collect()
      }
      assert(e.getMessage.contains(error))
  }
}

case class TestSampleCallStats(
    callRate: Double,
    nCalled: Long,
    nUncalled: Long,
    nHomRef: Long,
    nHet: Long,
    nHomVar: Long,
    nSnp: Long,
    nInsertion: Long,
    nDeletion: Long,
    nTransition: Long,
    nTransversion: Long,
    nSpanningDeletion: Long,
    rTiTv: Double,
    rInsertionDeletion: Double,
    rHetHomVar: Double)
