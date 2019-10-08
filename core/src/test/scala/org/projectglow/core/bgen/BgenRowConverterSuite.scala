package org.projectglow.core.bgen

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.projectglow.core.vcf.BgenRow
import org.projectglow.vcf.{BgenGenotype, BgenRow}

import com.databricks.vcf.{BgenGenotype, BgenRow}

class BgenRowConverterSuite extends BgenConverterBaseTest {

  val sourceName = "bgen"

  def compareVcfToBgen(
      testBgen: String,
      testVcf: String,
      bitsPerProb: Int,
      defaultPhasing: Boolean = false) {
    val sess = spark
    import sess.implicits._

    val bgenDs = spark
      .read
      .format(sourceName)
      .schema(BgenRow.schema)
      .load(testBgen)
      .as[BgenRow]
    val vcfDs = spark
      .read
      .format("vcf")
      .option("includeSampleIds", true)
      .load(testVcf)

    val converter = new InternalRowToBgenRowConverter(vcfDs.schema, 10, 2, defaultPhasing)
    val encoder = RowEncoder.apply(vcfDs.schema)

    bgenDs
      .sort("contigName", "start")
      .collect
      .zip(vcfDs.sort("contigName", "start").collect)
      .foreach {
        case (br, vr) =>
          checkBgenRowsEqual(br, converter.convert(encoder.toRow(vr)), false, bitsPerProb)
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
    val encoder = RowEncoder.apply(BgenRow.schema)

    val df = Seq(v).toDF
    if (shouldThrow) {
      assertThrows[IllegalStateException](converter.convert(encoder.toRow(df.collect.head)))
    } else {
      val row = converter.convert(encoder.toRow(df.collect.head))
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
    val encoder = RowEncoder.apply(BgenRow.schema)

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

    val row = Seq(mixedRow).toDF.collect.head
    assertThrows[IllegalStateException](converter.convert(encoder.toRow(row)))
  }
}
