package org.projectglow.core.tertiary

import org.apache.spark.sql.{DataFrame, Row}

import org.projectglow.core.common.VCFRow
import org.projectglow.core.sql.HLSBaseTest

class SampleQcExprsSuite extends HLSBaseTest {
  lazy val testVcf = s"$testDataHome/1000G.phase3.broad.withGenotypes.chr20.10100000.vcf"
  lazy val na12878 = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy private val sess = {
    // Set small partition size so that `merge` code path is implemented
    spark.conf.set("spark.sql.files.maxPartitionBytes", 512)
    spark
  }

  test("high level test") {
    import sess.implicits._
    val df = spark
      .read
      .format("vcf")
      .option("includeSampleIds", true)
      .load(na12878)
    val stats = df
      .selectExpr("sample_call_summary_stats(genotypes, referenceAllele, alternateAlleles) as qc")
      .selectExpr("explode(qc) as (sampleId, sqc)")
      .selectExpr("sampleId", "expand_struct(sqc)")
      .as[TestSampleCallStats]
      .head

    // Golden value is from Hail 0.2.12
    val expected = TestSampleCallStats(
      "NA12878",
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

  private def readVcf(path: String): DataFrame = {
    spark
      .read
      .format("vcf")
      .option("includeSampleIds", true)
      .load(path)
  }

  private def toCallStats(df: DataFrame): Seq[TestSampleCallStats] = {
    import sess.implicits._
    df.selectExpr("sample_call_summary_stats(genotypes, referenceAllele, alternateAlleles) as qc")
      .selectExpr("explode(qc) as (sampleId, sqc)")
      .selectExpr("sampleId", "expand_struct(sqc)")
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

  test("dp stats") {
    import sess.implicits._
    val stats = readVcf(na12878)
      .selectExpr("sample_dp_summary_stats(genotypes) as stats")
      .selectExpr("explode(stats) as (sampleId, stats)")
      .selectExpr("sampleId", "expand_struct(stats)")
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
      .selectExpr("explode(stats) as (sampleId, stats)")
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
      .selectExpr("explode(stats) as (sampleId, stats)")
      .selectExpr("sampleId", "expand_struct(stats)")
      .as[ArraySummaryStats]
      .head
    assert(stats.min.get ~== 3 relTol 0.2)
    assert(stats.max.get ~== 99 relTol 0.2)
    assert(stats.mean.get ~== 89.2 relTol 0.2)
    assert(stats.stdDev.get ~== 23.2 relTol 0.2)
  }
}

case class TestSampleCallStats(
    sampleId: String,
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
