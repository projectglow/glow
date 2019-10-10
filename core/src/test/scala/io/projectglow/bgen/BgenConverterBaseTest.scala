package io.projectglow.bgen

import io.projectglow.common.BgenRow
import io.projectglow.common.BgenRow
import io.projectglow.sql.GlowBaseTest
import io.projectglow.sql.GlowBaseTest

trait BgenConverterBaseTest extends GlowBaseTest {

  val testRoot = s"$testDataHome/bgen"

  // The error in a probability stored using the rounding rule is 1/(2**B - 1)
  def checkBgenRowsEqual(
      trueRow: BgenRow,
      testRow: BgenRow,
      strict: Boolean,
      bitsPerProb: Int): Unit = {
    val tolGivenBitsPerProb = 1.0 / ((1L << bitsPerProb) - 1)

    if (strict) {
      assert(trueRow.names == testRow.names)
    } else {
      // QCTools incorrectly separates IDs with commas instead of semicolons when exporting to VCF
      assert(
        trueRow.names.flatMap(_.split(",")).filter(n => n.nonEmpty && n != ".").distinct.sorted ==
        testRow.names.flatMap(_.split(",")).filter(n => n.nonEmpty && n != ".").distinct.sorted
      )
    }
    assert(trueRow.copy(names = Nil, genotypes = Nil) == testRow.copy(names = Nil, genotypes = Nil))
    assert(trueRow.genotypes.length == testRow.genotypes.length)
    trueRow.genotypes.zip(testRow.genotypes).foreach {
      case (oGt, nGt) =>
        assert(
          oGt.sampleId == nGt.sampleId || (oGt.sampleId.isEmpty && nGt
            .sampleId
            .get
            .startsWith("NA"))
        )
        if (oGt.posteriorProbabilities.nonEmpty || nGt.posteriorProbabilities.nonEmpty) {
          assert(oGt.phased == nGt.phased)
          assert(oGt.ploidy == nGt.ploidy)
          assert(oGt.posteriorProbabilities.length == nGt.posteriorProbabilities.length)

          oGt.posteriorProbabilities.zip(nGt.posteriorProbabilities).foreach {
            case (oPp, nPp) =>
              if (strict) {
                assert(oPp ~== nPp absTol tolGivenBitsPerProb)
              } else {
                // Account for truncation in VCF text representation
                val precision = math.min(
                  oPp.toString.split(".").lift(1).map(_.length).getOrElse(0),
                  nPp.toString.split(".").lift(1).map(_.length).getOrElse(0)
                )
                val impO =
                  BigDecimal(oPp).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
                val impN =
                  BigDecimal(nPp).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
                assert(impO ~== impN absTol tolGivenBitsPerProb)
              }
          }
        }
    }
  }
}
