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

import java.io.FileNotFoundException

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import io.projectglow.common.{PlinkGenotype, PlinkRow, VariantSchemas}
import io.projectglow.sql.GlowBaseTest
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

class PlinkReaderSuite extends GlowBaseTest {
  private val testRoot = s"$testDataHome/plink"

  test("Read PLINK files") {
    val sess = spark
    import sess.implicits._

    val plinkRows = spark
      .read
      .format("plink")
      .option("bim_delimiter", "\t")
      .load(s"$testRoot/small-test/small-test.bed")
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
            sampleId = "1_1",
            calls = Seq(0, 0)
          ),
          PlinkGenotype(
            sampleId = "2_1",
            calls = Seq(0, 1)
          ),
          PlinkGenotype(
            sampleId = "3_1",
            calls = Seq(1, 1)
          ),
          PlinkGenotype(
            sampleId = "4_1",
            calls = Seq(-1, -1)
          ),
          PlinkGenotype(
            sampleId = "5_1",
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
            sampleId = "1_1",
            calls = Seq(0, 0)
          ),
          PlinkGenotype(
            sampleId = "2_1",
            calls = Seq(0, 1)
          ),
          PlinkGenotype(
            sampleId = "3_1",
            calls = Seq(0, 0)
          ),
          PlinkGenotype(
            sampleId = "4_1",
            calls = Seq(0, 1)
          ),
          PlinkGenotype(
            sampleId = "5_1",
            calls = Seq(0, 1)
          )
        )
      ))
  }

  test("Missing FAM") {
    val df = spark
      .read
      .format("plink")
      .load(s"$testRoot/no-fam/small-test.bed")
    assertThrows[FileNotFoundException] {
      df.collect()
    }
  }

  test("Non-standard FAM path") {
    val df = spark
      .read
      .format("plink")
      .option("bim_delimiter", "\t")
      .option("fam", s"$testRoot/small-test/small-test.fam")
      .load(s"$testRoot/no-fam/small-test.bed")
    // Should not throw an error
    df.collect()
  }

  test("Missing BIM") {
    val df = spark
      .read
      .format("plink")
      .load(s"$testRoot/no-bim/small-test.bed")
    assertThrows[FileNotFoundException] {
      df.collect()
    }
  }

  test("Non-standard BIM path") {
    val df = spark
      .read
      .format("plink")
      .option("bim_delimiter", "\t")
      .option("bim", s"$testRoot/small-test/small-test.bim")
      .load(s"$testRoot/no-bim/small-test.bed")
    // Should not throw an error
    df.collect()
  }

  test("Wrong BIM delimiter") {
    val e = intercept[TreeNodeException[LogicalPlan]] {
      spark
        .read
        .format("plink")
        .load(s"$testRoot/small-test/small-test.bed")
        .sort("contigName")
        .collect
    }
    assert(e.getCause.getMessage.contains("Failed while parsing BIM file"))
  }

  test("Wrong FAM delimiter") {
    val e = intercept[IllegalArgumentException] {
      spark
        .read
        .format("plink")
        .option("fam_delimiter", "\t")
        .option("bim_delimiter", "\t")
        .load(s"$testRoot/small-test/small-test.bed")
        .collect()
    }
    assert(e.getMessage.contains("Failed while parsing FAM file"))
  }

  test("Read compared to VCF") {
    // The PLINK schema is a sub-set of the VCF schema, with the addition of a position field
    val commonVcfPlinkSchema =
      StructType(VariantSchemas.plinkSchema.filter(_.name != VariantSchemas.positionField.name))
    val vcfRows = spark
      .read
      .format("vcf")
      .schema(commonVcfPlinkSchema)
      .load(s"$testRoot/vcf/small-test.vcf")
      .sort("contigName")
    val plinkRows =
      spark
        .read
        .format("plink")
        .option("bim_delimiter", "\t")
        .schema(commonVcfPlinkSchema)
        .load(s"$testRoot/small-test/small-test.bed")
        .sort("contigName")
    assert(vcfRows.collect sameElements plinkRows.collect)
  }

  test("Read BED without magic bytes") {
    val e = intercept[SparkException] {
      spark
        .read
        .format("plink")
        .option("bim_delimiter", "\t")
        .load(s"$testRoot/small-test/small-test.fam")
        .collect()
    }
    assert(e.getMessage.contains("Magic bytes were not 006c,001b,0001"))
  }

  test("Read prematurely truncated BED") {
    val e = intercept[SparkException] {
      spark
        .read
        .format("plink")
        .option("bim_delimiter", "\t")
        .option("bim", s"$testRoot/small-test/small-test.bim")
        .option("fam", s"$testRoot/small-test/small-test.fam")
        .load(s"$testRoot/malformed/truncated.bed")
        .collect()
    }
    assert(e.getMessage.contains("BED file corrupted: could not read block 5 from 5 blocks"))
  }
}
