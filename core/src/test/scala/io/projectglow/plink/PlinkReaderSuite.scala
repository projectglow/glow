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

import org.apache.spark.{DebugFilesystem, SparkException}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

import io.projectglow.common.{PlinkGenotype, PlinkRow, VariantSchemas}
import io.projectglow.sql.GlowBaseTest

class PlinkReaderSuite extends GlowBaseTest {
  private val testRoot = s"$testDataHome/plink"
  private val fiveSamplesFiveVariants = s"$testRoot/five-samples-five-variants"
  private val bedBimFam = s"$fiveSamplesFiveVariants/bed-bim-fam"
  private val sourceName = "plink"

  test("Read PLINK files") {
    val sess = spark
    import sess.implicits._

    val plinkRows = spark
      .read
      .format(sourceName)
      .option("bim_delimiter", "\t")
      .load(s"$bedBimFam/test.bed")
      .sort("contigName")
      .as[PlinkRow]
      .collect

    assert(plinkRows.length == 5)
    DebugFilesystem.assertNoOpenStreams()

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
    assertThrows[FileNotFoundException] {
      df.collect()
    }
    DebugFilesystem.assertNoOpenStreams()
  }

  test("Non-standard FAM path") {
    val df = spark
      .read
      .format(sourceName)
      .option("bim_delimiter", "\t")
      .option("fam", s"$bedBimFam/test.fam")
      .load(s"$fiveSamplesFiveVariants/no-fam/test.bed")
    // Should not throw an error
    df.collect()
  }

  test("Missing BIM") {
    val df = spark
      .read
      .format(sourceName)
      .load(s"$fiveSamplesFiveVariants/no-bim/test.bed")
    assertThrows[FileNotFoundException] {
      df.collect()
    }
    DebugFilesystem.assertNoOpenStreams()
  }

  test("Non-standard BIM path") {
    val df = spark
      .read
      .format(sourceName)
      .option("bim_delimiter", "\t")
      .option("bim", s"$bedBimFam/test.bim")
      .load(s"$fiveSamplesFiveVariants/no-bim/test.bed")
    // Should not throw an error
    df.collect()
  }

  test("Wrong BIM delimiter") {
    val e = intercept[TreeNodeException[LogicalPlan]] {
      spark
        .read
        .format(sourceName)
        .load(s"$bedBimFam/test.bed")
        .sort("contigName")
        .collect
    }
    assert(e.getCause.getMessage.contains("Failed while parsing BIM file"))
  }

  test("Wrong FAM delimiter") {
    val e = intercept[IllegalArgumentException] {
      spark
        .read
        .format(sourceName)
        .option("fam_delimiter", "\t")
        .option("bim_delimiter", "\t")
        .load(s"$bedBimFam/test.bed")
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
      .load(s"$fiveSamplesFiveVariants/vcf/test.vcf")
      .sort("contigName")
    val plinkRows =
      spark
        .read
        .format(sourceName)
        .option("bim_delimiter", "\t")
        .schema(commonVcfPlinkSchema)
        .load(s"$bedBimFam/test.bed")
        .sort("contigName")
    assert(vcfRows.collect sameElements plinkRows.collect)
  }

  test("Read BED without magic bytes") {
    val e = intercept[SparkException] {
      spark
        .read
        .format(sourceName)
        .option("bim_delimiter", "\t")
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
        .option("bim_delimiter", "\t")
        .option("bim", s"$bedBimFam/test.bim")
        .option("fam", s"$bedBimFam/test.fam")
        .load(s"$fiveSamplesFiveVariants/malformed/test.bed")
        .collect()
    }
    assert(e.getMessage.contains("BED file corrupted: could not read block 5 from 5 blocks"))
  }

  test("Use IID only for sample ID") {
    val sess = spark
    import sess.implicits._

    val sampleIds = spark
      .read
      .format(sourceName)
      .option("bim_delimiter", "\t")
      .option("merge-fid-iid", "false")
      .load(s"$bedBimFam/test.bed")
      .select("genotypes.sampleId")
      .as[Seq[String]]
      .limit(1)
      .collect
      .head

    assert(sampleIds == Seq("ind1", "ind2", "ind3", "ind4", "ind5"))
  }

  test("Invalid arg for merge-fid-iid") {
    val e = intercept[IllegalArgumentException] {
      spark
        .read
        .format(sourceName)
        .option("bim_delimiter", "\t")
        .option("merge-fid-iid", "hello")
        .load(s"$bedBimFam/test.bed")
        .collect()
    }
    assert(e.getMessage.contains("Value for merge-fid-iid must be [true, false]. Provided: hello"))
  }

  test("Plink file format does not support writing") {
    val df = spark
      .read
      .format(sourceName)
      .option("bim_delimiter", "\t")
      .load(s"$bedBimFam/test.bed")
    val e = intercept[UnsupportedOperationException] {
      df.write.format(sourceName).save("noop")
    }
    assert(e.getMessage.contains("PLINK data source does not support writing sharded files"))
  }

  case class WeirdGenotype(animal: String)
  case class WeirdVariant(species: String, genotypes: Seq[WeirdGenotype])
  test("be permissive if schema includes fields that can't be derived from PLINK files") {
    spark
      .read
      .schema(ScalaReflection.schemaFor[WeirdVariant].dataType.asInstanceOf[StructType])
      .format(sourceName)
      .option("bim_delimiter", "\t")
      .load(s"$bedBimFam/test.bed")
    // No error expected
  }

}
