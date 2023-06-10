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

package io.projectglow.plink

import java.io.{EOFException, FileNotFoundException}

import org.apache.spark.{SparkException}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType

import io.projectglow.common.{PlinkGenotype, PlinkRow, VariantSchemas}
import io.projectglow.sql.GlowBaseTest

class PlinkReaderSuite extends GlowBaseTest {
  private val testRoot = s"$testDataHome/plink"
  private val fiveSamplesFiveVariants = s"$testRoot/five-samples-five-variants"
  private val bedBimFam = s"$fiveSamplesFiveVariants/bed-bim-fam"
  private val sourceName = "plink"

  /**
   * Checks if an exception with an expected type and message exists in a stack of exceptions.
   *
   * @param exception The top of the exception stack
   * @param msg Message contained in the exception
   * @tparam T Expected exception type
   * @return True if found, False otherwise
   */
  def checkExceptionContains[T <: Throwable](exception: Throwable, msg: String = ""): Boolean = {
    var e = exception
    while (true) {
      if (e.isInstanceOf[T] && e.getMessage.contains(msg)) {
        return true
      } else {
        if (e.getCause != null) {
          e = e.getCause
        } else {
          return false
        }
      }
    }
    false
  }

  test("Read PLINK files") {
    val sess = spark
    import sess.implicits._

    val plinkRows = spark
      .read
      .format(sourceName)
      .load(s"$bedBimFam/test.bed")
      .sort("contigName")
      .as[PlinkRow]
      .collect

    assert(plinkRows.length == 5)

    val snp1 = plinkRows.head
    assert(
      snp1 == PlinkRow(
        contigName = "1",
        position = 0.0,
        start = 9,
        end = 10,
        names = Seq("snp1"),
        referenceAllele = "A",
        alternateAlleles = Seq("C"),
        genotypes = Seq(
          PlinkGenotype(
            sampleId = "fam1_ind1",
            calls = Seq(0, 0)
          ),
          PlinkGenotype(
            sampleId = "fam2_ind2",
            calls = Seq(0, 1)
          ),
          PlinkGenotype(
            sampleId = "fam3_ind3",
            calls = Seq(1, 1)
          ),
          PlinkGenotype(
            sampleId = "fam4_ind4",
            calls = Seq(-1, -1)
          ),
          PlinkGenotype(
            sampleId = "fam5_ind5",
            calls = Seq(-1, -1)
          )
        )
      ))

    val snp5 = plinkRows.last
    assert(
      snp5 == PlinkRow(
        contigName = "26", // MT
        position = 2.5,
        start = 49,
        end = 50,
        names = Seq("snp5"),
        referenceAllele = "A",
        alternateAlleles = Seq("C"),
        genotypes = Seq(
          PlinkGenotype(
            sampleId = "fam1_ind1",
            calls = Seq(0, 0)
          ),
          PlinkGenotype(
            sampleId = "fam2_ind2",
            calls = Seq(0, 1)
          ),
          PlinkGenotype(
            sampleId = "fam3_ind3",
            calls = Seq(0, 0)
          ),
          PlinkGenotype(
            sampleId = "fam4_ind4",
            calls = Seq(0, 1)
          ),
          PlinkGenotype(
            sampleId = "fam5_ind5",
            calls = Seq(0, 1)
          )
        )
      ))
  }

  test("Missing FAM") {
    val df = spark
      .read
      .format(sourceName)
      .load(s"$fiveSamplesFiveVariants/no-fam/test.bed")
    val e = intercept[SparkException] {
      df.collect()
    }
    checkExceptionContains[FileNotFoundException](e)
  }

  test("Missing BIM") {
    val df = spark
      .read
      .format(sourceName)
      .load(s"$fiveSamplesFiveVariants/no-bim/test.bed")
    val e = intercept[SparkException] {
      df.collect()
    }
    checkExceptionContains[FileNotFoundException](e)
  }

  test("Wrong BIM delimiter") {
    val e = intercept[SparkException] {
      spark
        .read
        .format(sourceName)
        .option("bimDelimiter", " ")
        .load(s"$bedBimFam/test.bed")
        .sort("contigName")
        .collect
    }
    checkExceptionContains[IllegalArgumentException](e, "Failed while parsing BIM file")
  }

  test("Wrong FAM delimiter") {
    val e = intercept[SparkException] {
      spark
        .read
        .format(sourceName)
        .option("famDelimiter", "\t")
        .load(s"$bedBimFam/test.bed")
        .collect()
    }
    assert(e.getMessage.contains("Failed while parsing FAM file"))
  }

  test("Read subset of columns") {
    val rows = spark
      .read
      .format(sourceName)
      .load(s"$bedBimFam/test.bed")
      .select("contigName", "start", "genotypes.sampleId")
      .orderBy("contigName")
      .collect
    assert(
      rows.head == Row(
        "1",
        9,
        Seq("fam1_ind1", "fam2_ind2", "fam3_ind3", "fam4_ind4", "fam5_ind5")))
  }

  test("Read compared to VCF") {
    // The PLINK schema is a sub-set of the VCF schema, with the addition of a position field
    val commonVcfPlinkSchema =
      StructType(
        VariantSchemas.plinkSchema(true).filter(_.name != VariantSchemas.positionField.name))
    val vcfRows = spark
      .read
      .format("vcf")
      .schema(commonVcfPlinkSchema)
      .load(s"$fiveSamplesFiveVariants/vcf/test.vcf")
      .sort("contigName")
    val plinkRows =
      spark
        .read
        .format(sourceName)
        .schema(commonVcfPlinkSchema)
        .load(s"$bedBimFam/test.bed")
        .sort("contigName")
    assert(vcfRows.collect.sameElements(plinkRows.collect))
  }

  test("Read BED without magic bytes") {
    val e = intercept[SparkException] {
      spark
        .read
        .format(sourceName)
        .load(s"$bedBimFam/test.fam")
        .collect()
    }
    assert(e.getMessage.contains("Magic bytes were not 006c,001b,0001"))
  }

  test("Read prematurely truncated BED") {
    val e = intercept[SparkException] {
      spark
        .read
        .format(sourceName)
        .load(s"$fiveSamplesFiveVariants/malformed/test.bed")
        .collect()
    }
    checkExceptionContains[EOFException](e)
  }

  test("Use IID only for sample ID") {
    val sess = spark
    import sess.implicits._

    val sampleIds = spark
      .read
      .format(sourceName)
      .option("mergeFidIid", "false")
      .load(s"$bedBimFam/test.bed")
      .select("genotypes.sampleId")
      .as[Seq[String]]
      .limit(1)
      .collect
      .head

    assert(sampleIds == Seq("ind1", "ind2", "ind3", "ind4", "ind5"))
  }

  test("Invalid arg for mergeFidIid") {
    val e = intercept[SparkException] {
      spark
        .read
        .format(sourceName)
        .option("mergeFidIid", "hello")
        .load(s"$bedBimFam/test.bed")
        .collect()
    }
    checkExceptionContains[IllegalArgumentException](
      e,
      "Value for mergeFidIid must be [true, false]. Provided: hello")
  }

  test("PLINK file format does not support writing") {
    val df = spark
      .read
      .format(sourceName)
      .load(s"$bedBimFam/test.bed")
    val e = intercept[UnsupportedOperationException] {
      df.write.format(sourceName).save("noop")
    }
    assert(e.getMessage.contains("PLINK data source does not support writing"))
  }

  case class WeirdGenotype(animal: String)
  case class WeirdVariant(species: String, genotypes: Seq[WeirdGenotype])

  test("Be permissive if schema includes fields that can't be derived from PLINK files") {
    val rows = spark
      .read
      .schema(ScalaReflection.schemaFor[WeirdVariant].dataType.asInstanceOf[StructType])
      .format(sourceName)
      .load(s"$bedBimFam/test.bed")
      .collect

    rows.foreach { r =>
      assert(r == Row(null, Seq(Row(null), Row(null), Row(null), Row(null), Row(null))))
    }
  }

  test("Accept glob path") {
    val rows = spark.read.format(sourceName).load(s"$bedBimFam/*.bed").collect
    assert(rows.length == 5)
  }

  test("Support not having sample IDs") {
    val df = spark
      .read
      .format(sourceName)
      .option("includeSampleIds", "false")
      .load(s"$bedBimFam/test.bed")

    val schema = df
      .withColumn("g", expr("genotypes[0]"))
      .selectExpr("g.*")
      .schema

    assert(!schema.exists(_.name == "sampleId"))
  }

  def getVariantIdxStartNum(fileStart: Long, fileLength: Long, blockSize: Int): (Int, Long, Int) = {
    val firstVariantIdx = PlinkFileFormat.getFirstVariantIdx(fileStart, blockSize)
    val firstVariantStart = PlinkFileFormat.getVariantStart(firstVariantIdx, blockSize)
    val numVariants =
      PlinkFileFormat.getNumVariants(fileStart, fileLength, firstVariantStart, blockSize)
    (firstVariantIdx, firstVariantStart, numVariants)
  }

  test("Full file") {
    assert(getVariantIdxStartNum(0, 13, 2) == (0, 3, 5))
  }

  test("Start in magic block") {
    assert(getVariantIdxStartNum(1, 12, 5) == (0, 3, 2))
  }

  test("Start at beginning of first block") {
    assert(getVariantIdxStartNum(3, 10, 2) == (0, 3, 5))
  }

  test("Start in middle of first block") {
    assert(getVariantIdxStartNum(4, 9, 2) == (1, 5, 4))
  }

  test("Zero-variant slice") {
    assert(getVariantIdxStartNum(6, 1, 2) == (2, 7, 0))
  }

  test("One-variant slice") {
    assert(getVariantIdxStartNum(7, 1, 2) == (2, 7, 1))
  }

  test("Multi-variant slice") {
    assert(getVariantIdxStartNum(7, 6, 2) == (2, 7, 3))
  }

  test("Get block size") {
    (1 to 4).foreach { s =>
      assert(PlinkFileFormat.getBlockSize(s) == 1)
    }
    (5 to 8).foreach { s =>
      assert(PlinkFileFormat.getBlockSize(s) == 2)
    }
  }

  test("Big PLINK files") {
    val num1KgSamples = 2504
    val blockSize = PlinkFileFormat.getBlockSize(num1KgSamples)
    assert(blockSize == 626)

    val variantIdx = 25484379
    val variantStart = PlinkFileFormat.getVariantStart(variantIdx, blockSize)
    assert(variantStart == 15953221257L)
  }

}
