package org.projectglow.tertiary

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{LongType, StringType, StructType}

import org.projectglow.sql.HLSBaseTest

class LiftOverCoordinatesExprSuite extends HLSBaseTest {
  val requiredBaseSchema: StructType = new StructType()
    .add("contigName", StringType)
    .add("start", LongType)
    .add("end", LongType)

  val chainFile = s"$testDataHome/liftover/hg38ToHg19.over.chain.gz"

  private def liftOverAndCompare(
      inputDf: DataFrame,
      crossMappedDf: DataFrame,
      crossUnmappedDf: DataFrame,
      minMatchOpt: Option[Double]): Unit = {
    val sess = spark
    import sess.implicits._

    val outputDf = if (minMatchOpt.isDefined) {
      inputDf
        .withColumn("minMatchRatio", expr(s"${minMatchOpt.get}"))
        .withColumn(
          "lifted",
          expr(s"lift_over_coordinates(contigName, start, end, '$chainFile', minMatchRatio)"))
        .drop("minMatchRatio")
    } else {
      inputDf
        .withColumn("lifted", expr(s"lift_over_coordinates(contigName, start, end, '$chainFile')"))
    }
    val liftedDf =
      outputDf
        .filter("lifted is not null")
        .withColumn("contigName", $"lifted.contigName")
        .withColumn("start", $"lifted.start")
        .withColumn("end", $"lifted.end")
        .drop("lifted")
    val unliftedDf = outputDf.filter("lifted is null").drop("lifted")

    val liftedRows = liftedDf.collect()
    val crossMappedRows = crossMappedDf.collect()
    assert(liftedRows.length == crossMappedRows.length)
    liftedRows.zip(crossMappedRows).foreach { case (r1, r2) => assert(r1 == r2) }

    val unliftedRows = unliftedDf.collect()
    val crossUnmappedRows = crossUnmappedDf.collect()
    assert(unliftedRows.length == crossUnmappedRows.length)
    unliftedRows.zip(crossUnmappedRows).foreach { case (r1, r2) => assert(r1 == r2) }
  }

  private def readVcf(vcfFile: String): DataFrame = {
    spark
      .read
      .format("vcf")
      .load(vcfFile)
      .select("contigName", "start", "end")
  }

  private def compareLiftedVcf(
      testVcf: String,
      crossMappedVcf: String,
      crossUnmappedVcf: String,
      minMatchOpt: Option[Double] = None): Unit = {
    val inputDf = readVcf(testVcf)
    val crossMappedDf = readVcf(crossMappedVcf)
    val crossUnmappedDf = readVcf(crossUnmappedVcf)
    liftOverAndCompare(inputDf, crossMappedDf, crossUnmappedDf, minMatchOpt)
  }

  private def readBed(bedFile: String): DataFrame = {
    spark
      .read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .schema(requiredBaseSchema)
      .load(bedFile)
  }

  private def compareLiftedBed(
      testBed: String,
      crossMappedBed: String,
      crossUnmappedBed: String,
      minMatchOpt: Option[Double] = None): Unit = {
    val inputDf = readBed(testBed)
    val crossMappedDf = readBed(crossMappedBed)
    val crossUnmappedDf = readBed(crossUnmappedBed)
    liftOverAndCompare(inputDf, crossMappedDf, crossUnmappedDf, minMatchOpt)
  }

  test("Basic") {
    compareLiftedVcf(
      s"$testDataHome/combined.chr20_18210071_18210093.g.vcf",
      s"$testDataHome/liftover/lifted.combined.chr20_18210071_18210093.g.vcf",
      s"$testDataHome/liftover/failed.combined.chr20_18210071_18210093.g.vcf",
      None
    )
  }

  test("Some failures") {
    compareLiftedVcf(
      s"$testDataHome/liftover/unlifted.test.vcf",
      s"$testDataHome/liftover/lifted.test.vcf",
      s"$testDataHome/liftover/failed.test.vcf",
      None)
  }

  test("High min match") {
    compareLiftedBed(
      s"$testDataHome/liftover/unlifted.test.bed",
      s"$testDataHome/liftover/lifted.minMatch05.test.bed",
      s"$testDataHome/liftover/failed.minMatch05.test.bed",
      Some(0.5)
    )
  }

  test("Low min match") {
    compareLiftedBed(
      s"$testDataHome/liftover/unlifted.test.bed",
      s"$testDataHome/liftover/lifted.minMatch001.test.bed",
      s"$testDataHome/liftover/failed.minMatch001.test.bed",
      Some(0.01)
    )
  }

  test("Do not cache chain file") {
    val inputDf = readBed(s"$testDataHome/liftover/unlifted.test.bed")
    inputDf.selectExpr(s"lift_over_coordinates(contigName, start, end, '$chainFile')").collect()
    val ex = intercept[SparkException](
      inputDf.selectExpr("lift_over_coordinates(contigName, start, end, 'fakeChainFile')").collect()
    )
    assert(ex.getMessage.contains("htsjdk.samtools.SAMException: Cannot read non-existent file"))
  }

  test("Null contigName") {
    val inputDf =
      readBed(s"$testDataHome/liftover/unlifted.test.bed")
    val outputDf =
      inputDf.withColumn("lifted", expr(s"lift_over_coordinates(null, start, end, '$chainFile')"))
    assert(outputDf.filter("lifted is null").count == outputDf.count)
  }

  test("Null start") {
    val inputDf =
      readBed(s"$testDataHome/liftover/unlifted.test.bed")
    val outputDf =
      inputDf.withColumn(
        "lifted",
        expr(s"lift_over_coordinates(contigName, null, end, '$chainFile')"))
    assert(outputDf.filter("lifted is null").count == outputDf.count)
  }

  test("Null end") {
    val inputDf =
      readBed(s"$testDataHome/liftover/unlifted.test.bed")
    val outputDf =
      inputDf.withColumn(
        "lifted",
        expr(s"lift_over_coordinates(contigName, start, null, '$chainFile')"))
    assert(outputDf.filter("lifted is null").count == outputDf.count)
  }

  test("Null minMatchRatio") {
    val inputDf =
      readBed(s"$testDataHome/liftover/unlifted.test.bed")
    val outputDf =
      inputDf.withColumn(
        "lifted",
        expr(s"lift_over_coordinates(contigName, start, end, '$chainFile', null)"))
    assert(outputDf.filter("lifted is null").count == outputDf.count)
  }
}
