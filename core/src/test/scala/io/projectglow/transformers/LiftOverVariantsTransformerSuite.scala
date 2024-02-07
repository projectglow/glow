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

package io.projectglow.transformers

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import picard.vcf.LiftoverVcf

import io.projectglow.Glow
import io.projectglow.sql.GlowBaseTest
import io.projectglow.vcf.VCFConverterBaseTest

class LiftOverVariantsTransformerSuite extends GlowBaseTest with VCFConverterBaseTest {
  val picardTestDataHome = s"$testDataHome/liftover/picard"
  val CHAIN_FILE = s"$testDataHome/liftover/hg38ToHg19.over.chain.gz"
  val REFERENCE_FILE = s"$testDataHome/liftover/hg19.chr20.fa.gz"

  val requiredBaseSchema: StructType = new StructType()
    .add("contigName", StringType)
    .add("start", LongType)
    .add("end", LongType)
    .add("referenceAllele", StringType)

  private def liftOverAndCompare(
      inputDf: DataFrame,
      picardLiftedDf: DataFrame,
      picardFailedDf: DataFrame,
      chainFile: String,
      referenceFile: String,
      minMatchOpt: Option[Double]): Unit = {

    val minMatchMap = if (minMatchOpt.isDefined) {
      Map("minMatchRatio" -> minMatchOpt.get.toString)
    } else {
      Map.empty
    }
    val outputDf = Glow
      .transform(
        "lift_over_variants",
        inputDf,
        Map("chainFile" -> chainFile, "referenceFile" -> referenceFile) ++ minMatchMap)
      .orderBy("contigName", "start")

    val liftedDf =
      outputDf
        .filter("liftOverStatus.success = true")
        .select(picardLiftedDf.schema.fieldNames.head, picardLiftedDf.schema.fieldNames.tail: _*)
    val failedDf =
      outputDf
        .filter("liftOverStatus.success = false")
        .select(
          "contigName",
          "start",
          "end",
          "referenceAllele",
          "alternateAlleles",
          "liftOverStatus.errorMessage")

    val liftedRows = liftedDf.collect()
    val picardLiftedRows = picardLiftedDf.orderBy("contigName", "start").collect()

    assert(liftedDf.schema == picardLiftedDf.schema)
    assert(liftedRows.length == picardLiftedRows.length)
    liftedRows.zip(picardLiftedRows).foreach { case (r1, r2) => assert(r1 == r2) }

    val failedRows = failedDf.collect()
    val picardFailedRows =
      picardFailedDf
        .selectExpr(
          "contigName",
          "start",
          "end",
          "referenceAllele",
          "alternateAlleles",
          "filters[0]")
        .orderBy("contigName", "start")
        .collect()
    assert(failedRows.length == picardFailedRows.length)
    failedRows.zip(picardFailedRows).foreach {
      case (r1, r2) =>
        val numFields = r1.length
        var i = 0
        while (i < numFields - 1) {
          assert(r1(i) == r2(i))
          i += 1
        }
        assert(r1.getString(numFields - 1).contains(r2.getString(numFields - 1)))
    }
  }

  private def readVcf(vcfFile: String): DataFrame = {
    spark
      .read
      .format("vcf")
      .load(vcfFile)
  }

  private def compareLiftedVcf(
      testVcf: String,
      picardLiftedVcf: String,
      picardFailedVcf: String,
      chainFile: String = CHAIN_FILE,
      referenceFile: String = REFERENCE_FILE,
      minMatchOpt: Option[Double] = None): Unit = {
    val inputDf = readVcf(testVcf)

    val picardLiftedDf = readVcf(picardLiftedVcf)
    val picardFailedDf = readVcf(picardFailedVcf)
    liftOverAndCompare(
      inputDf,
      picardLiftedDf,
      picardFailedDf,
      chainFile,
      referenceFile,
      minMatchOpt)
  }

  test("Basic") {
    compareLiftedVcf(
      s"$testDataHome/combined.chr20_18210071_18210093.g.vcf",
      s"$testDataHome/liftover/lifted.combined.chr20_18210071_18210093.g.vcf",
      s"$testDataHome/liftover/failed.combined.chr20_18210071_18210093.g.vcf" // No failures
    )
  }

  test("Some failures") {
    compareLiftedVcf(
      s"$testDataHome/liftover/unlifted.test.vcf",
      s"$testDataHome/liftover/lifted.test.vcf",
      s"$testDataHome/liftover/failed.test.vcf" // NoTarget
    )
  }

  test("Don't change original fields") {
    val sess = spark
    import sess.implicits._

    val inputDf = readVcf(s"$testDataHome/liftover/unlifted.test.vcf")
      .withColumn("id", functions.monotonically_increasing_id)
    val outputDf = Glow
      .transform(
        "lift_over_variants",
        inputDf,
        Map("chainFile" -> CHAIN_FILE, "referenceFile" -> REFERENCE_FILE))
      .orderBy("id")
    assert(
      outputDf.select("id").as[Long].collect sameElements inputDf.select("id").as[Long].collect)
  }

  requiredBaseSchema.indices.foreach { idx =>
    test(s"Try lifting with insufficient fields, drop ${requiredBaseSchema(idx)}") {
      val inputDf =
        spark
          .read
          .format("vcf")
          .schema(
            new StructType(requiredBaseSchema
              .fields
              .take(idx) ++ requiredBaseSchema.fields.takeRight(requiredBaseSchema.size - idx - 1)))
          .load(s"$testDataHome/combined.chr20_18210071_18210093.g.vcf")
      assertThrows[IllegalArgumentException] {
        Glow
          .transform(
            "lift_over_variants",
            inputDf,
            Map("chainFile" -> CHAIN_FILE, "referenceFile" -> REFERENCE_FILE))
      }
    }
  }

  Seq(
    "testLiftoverBiallelicIndels.vcf",
    "testLiftoverMultiallelicIndels.vcf", // ReverseComplementedIndel
    "testLiftoverSwapRefAltVariants.vcf" // SwappedRefAlt
  ).foreach {
    case baseVcf =>
      test(s"Liftover reverse strand $baseVcf") {
        compareLiftedVcf(
          s"$picardTestDataHome/$baseVcf",
          s"$picardTestDataHome/lifted.$baseVcf",
          s"$picardTestDataHome/failed.$baseVcf",
          s"$picardTestDataHome/test.over.chain",
          s"$picardTestDataHome/dummy.reference.fasta",
          Some(1.0)
        )
      }
  }

  test("Simple indels") {
    compareLiftedVcf(
      s"$picardTestDataHome/testLiftoverIndelNoFlip.vcf",
      s"$picardTestDataHome/lifted.testLiftoverIndelNoFlip.vcf",
      s"$picardTestDataHome/failed.testLiftoverIndelNoFlip.vcf",
      s"$picardTestDataHome/test.two.block.over.chain",
      s"$picardTestDataHome/dummy.two.block.reference.fasta",
      Some(.95)
    )
  }

  test("Indel flip") {
    compareLiftedVcf(
      s"$picardTestDataHome/testLiftoverIndelFlip.vcf",
      s"$picardTestDataHome/lifted.testLiftoverIndelFlip.vcf",
      s"$picardTestDataHome/failed.testLiftoverIndelFlip.vcf",
      s"$picardTestDataHome/test.over.chain",
      s"$picardTestDataHome/mini.reference.fasta",
      Some(1.0)
    )
  }

  test("Missing chain file") {
    val inputDf = readVcf(s"$testDataHome/combined.chr20_18210071_18210093.g.vcf")
    val ex = intercept[SparkException] {
      val outputDf =
        Glow.transform("lift_over_variants", inputDf, Map("referenceFile" -> REFERENCE_FILE))
      outputDf.count
    }
    assert(ex.getMessage.contains("Must provide chain file"))
  }

  test("Missing reference file") {
    val inputDf = readVcf(s"$testDataHome/combined.chr20_18210071_18210093.g.vcf")
    val ex = intercept[SparkException] {
      val outputDf =
        Glow.transform("lift_over_variants", inputDf, Map("chainFile" -> CHAIN_FILE))
      outputDf.count
    }
    assert(ex.getMessage.contains("Must provide reference file"))
  }

  test("No matching refseq") {
    val sess = spark
    import sess.implicits._

    // chr20 refseq for chr1 interval
    val inputDf = readVcf(s"$picardTestDataHome/testLiftoverBiallelicIndels.vcf")
    val outputDf = Glow.transform(
      "lift_over_variants",
      inputDf,
      Map("chainFile" -> s"$picardTestDataHome/test.over.chain", "referenceFile" -> REFERENCE_FILE))
    val distinctErrorMessages =
      outputDf.select("liftOverStatus.errorMessage").distinct.as[String].collect

    assert(distinctErrorMessages.length == 1)
    assert(distinctErrorMessages.head == LiftoverVcf.FILTER_NO_TARGET)
  }

  test("Reverse complemented bases don't match new reference") {
    compareLiftedVcf(
      s"$picardTestDataHome/testLiftoverBiallelicIndels.vcf",
      s"$picardTestDataHome/lifted.mismatchRefSeq.testLiftoverBiallelicIndels.vcf",
      s"$picardTestDataHome/failed.mismatchRefSeq.testLiftoverBiallelicIndels.vcf",
      s"$picardTestDataHome/test.over.chain",
      s"$picardTestDataHome/dummy2.reference.fasta",
      Some(1.0)
    )
  }

  test("Arrays flipped during ref/alt swap") {
    compareLiftedVcf(
      s"$testDataHome/liftover/unlifted.swapRefAltAndArrays.vcf",
      s"$testDataHome/liftover/lifted.swapRefAltAndArrays.vcf",
      s"$testDataHome/liftover/failed.swapRefAltAndArrays.vcf",
      s"$picardTestDataHome/test.over.chain",
      s"$picardTestDataHome/dummy.reference.fasta",
      Some(1.0)
    )
  }
}
