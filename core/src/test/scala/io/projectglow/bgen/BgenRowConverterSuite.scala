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

package io.projectglow.bgen

import org.apache.spark.sql.catalyst.encoders.RowEncoder

import io.projectglow.common.{BgenGenotype, BgenRow}

class BgenRowConverterSuite extends BgenConverterBaseTest {

  val sourceName = "bgen"

  def compareVcfToBgen(
      testBgen: String,
      testVcf: String,
      bitsPerProb: Int,
      defaultPhasing: Boolean = false) {
    val sess = spark
    import sess.implicits._

    val bgenRows = spark
      .read
      .format(sourceName)
      .schema(BgenRow.schema)
      .load(testBgen)
      .sort("contigName", "start", "names")
      .as[BgenRow]
      .collect
    val vcfDf = spark
      .read
      .format("vcf")
      .load(testVcf)
      .sort("contigName", "start", "names")
    val converter = new InternalRowToBgenRowConverter(vcfDf.schema, 10, 2, defaultPhasing)
    val vcfRows = vcfDf
      .queryExecution
      .toRdd
      .collect
      .map(converter.convert)

    assert(bgenRows.size == vcfRows.size)
    bgenRows.zip(vcfRows).foreach {
      case (br, vr) =>
        checkBgenRowsEqual(br, vr, false, bitsPerProb)
    }
  }

  test("unphased 8 bit") {
    compareVcfToBgen(s"$testRoot/example.8bits.bgen", s"$testRoot/example.8bits.vcf", 8)
  }

  test("unphased 16 bit (with missing samples)") {
    compareVcfToBgen(s"$testRoot/example.16bits.bgen", s"$testRoot/example.16bits.vcf", 16)
  }

  test("unphased 32 bit") {
    compareVcfToBgen(s"$testRoot/example.32bits.bgen", s"$testRoot/example.32bits.vcf", 32)
  }

  test("phased") {
    compareVcfToBgen(s"$testRoot/phased.16bits.bgen", s"$testRoot/phased.16bits.vcf", 16, true)
  }

  def inferPhasingOrPloidy(
      alternateAlleles: Seq[String],
      numPosteriorProbabilities: Int,
      phasedOpt: Option[Boolean],
      phased: Boolean,
      ploidyOpt: Option[Int],
      ploidy: Int,
      shouldThrow: Boolean = false,
      maxPloidy: Int = 10,
      defaultPloidy: Int = 2,
      defaultPhasing: Boolean = false): Unit = {
    val sess = spark
    import sess.implicits._

    val v = BgenRow(
      "chr1",
      10,
      11,
      Nil,
      "A",
      alternateAlleles,
      Seq(
        BgenGenotype(
          None,
          phasedOpt,
          ploidyOpt,
          (0 until numPosteriorProbabilities).map(_.toDouble)
        )
      )
    )

    val converter = new InternalRowToBgenRowConverter(
      BgenRow.schema,
      maxPloidy,
      defaultPloidy,
      defaultPhasing
    )

    val internalRow = convertToInternalRow(v)
    if (shouldThrow) {
      assertThrows[IllegalStateException](converter.convert(internalRow))
    } else {
      val row = converter.convert(internalRow)
      assert(row.genotypes.head.phased.get == phased)
      assert(row.genotypes.head.ploidy.get == ploidy)
    }
  }

  test("Infer phasing from ploidy") {
    // Unphased
    inferPhasingOrPloidy(Seq("T"), 3, None, false, Some(2), 2)
    inferPhasingOrPloidy(Seq("T", "C"), 10, None, false, Some(3), 3)

    // Phased
    inferPhasingOrPloidy(Seq("T"), 4, None, true, Some(2), 2)
    inferPhasingOrPloidy(Seq("T", "C"), 9, None, true, Some(3), 3)

    // Throw if nonsensical
    inferPhasingOrPloidy(Seq("T"), 2, None, false, Some(2), 2, true)
    inferPhasingOrPloidy(Seq("T"), 5, None, true, Some(2), 2, true)
  }

  test("If phasing ambiguous, use default phasing when inferring from ploidy") {
    // Default phasing is false
    inferPhasingOrPloidy(Seq("T"), 0, None, false, Some(0), 0)
    inferPhasingOrPloidy(Seq("T"), 2, None, false, Some(1), 1)
    inferPhasingOrPloidy(Seq("T"), 2, None, true, Some(1), 1, defaultPhasing = true)
  }

  test("Infer ploidy from phasing") {
    // Unphased
    inferPhasingOrPloidy(Seq("T"), 3, Some(false), false, None, 2)
    inferPhasingOrPloidy(Seq("T", "C"), 10, Some(false), false, None, 3)

    // Phased
    inferPhasingOrPloidy(Seq("T"), 4, Some(true), true, None, 2)
    inferPhasingOrPloidy(Seq("T", "C"), 9, Some(true), true, None, 3)

    // Throw if nonsensical
    inferPhasingOrPloidy(Seq("T", "C"), 9, Some(false), false, None, 3, true)
    inferPhasingOrPloidy(Seq("T"), 5, Some(true), true, None, 2, true)
  }

  test("If ploidy ambiguous, use default ploidy when inferring from phasing") {
    // Default ploidy is 2
    inferPhasingOrPloidy(Seq("T"), 0, Some(true), true, None, 2)
    inferPhasingOrPloidy(Seq("T", "C"), 0, Some(false), false, None, 3, defaultPloidy = 3)
  }

  test("If no ploidy or phasing info provided, use default ploidy") {
    // Default ploidy is 2
    inferPhasingOrPloidy(Seq("T"), 3, None, false, None, 2)
    inferPhasingOrPloidy(Seq("T"), 0, None, false, None, 0, defaultPloidy = 0)
    inferPhasingOrPloidy(Seq("T", "C"), 9, None, true, None, 3, defaultPloidy = 3)
  }

  test("Infer ploidy for unphased up to max") {
    // Max is 10
    inferPhasingOrPloidy(Seq("T", "C"), 66, Some(false), false, None, 10)
    inferPhasingOrPloidy(Seq("T"), 12, Some(false), false, None, 11, true)

    inferPhasingOrPloidy(Seq("T", "C"), 66, Some(false), false, None, 10, maxPloidy = 12)
  }

  test("Throws on mixed phasing") {
    val sess = spark
    import sess.implicits._

    val converter = new InternalRowToBgenRowConverter(BgenRow.schema, 10, 2, false)

    val mixedRow = BgenRow(
      "chr1",
      10,
      11,
      Nil,
      "A",
      Seq("T"),
      Seq(
        BgenGenotype(
          None,
          Some(true),
          Some(2),
          Nil
        ),
        BgenGenotype(
          None,
          Some(false),
          Some(2),
          Nil
        )
      )
    )

    val internalRow = convertToInternalRow(mixedRow)
    assertThrows[IllegalStateException](converter.convert(internalRow))
  }
}
