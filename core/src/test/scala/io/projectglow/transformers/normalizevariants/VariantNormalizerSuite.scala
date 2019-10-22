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

package io.projectglow.transformers.normalizevariants

import java.nio.file.Paths

import htsjdk.variant.variantcontext.Allele
import org.broadinstitute.hellbender.engine.ReferenceDataSource

import io.projectglow.common.GlowLogging
import io.projectglow.transformers.normalizevariants.VariantNormalizer._
import io.projectglow.sql.GlowBaseTest

class VariantNormalizerSuite extends GlowBaseTest with GlowLogging {

  lazy val sourceName: String = "vcf"
  lazy val testFolder: String = s"$testDataHome/variantnormalizer-test"
  lazy val vtTestReference = s"$testFolder/20_altered.fasta"

  /**
   * Tests realignAlleles method for given alleles and compares with the provided expected
   * outcome
   */
  def testRealignAlleles(
      referenceGenome: String,
      contig: String,
      origStart: Int,
      origEnd: Int,
      origAlleleStrings: Seq[String],
      expectedStart: Int,
      expectedEnd: Int,
      expectedAlleleString: Seq[String]): Unit = {

    val refGenomeDataSource = ReferenceDataSource.of(Paths.get(referenceGenome))

    val alleles = origAlleleStrings.take(1).map(Allele.create(_, true)) ++
      origAlleleStrings.drop(1).map(Allele.create(_))

    val reAlignedAlleles =
      realignAlleles(AlleleBlock(alleles, origStart, origEnd), refGenomeDataSource, contig)

    assert(reAlignedAlleles.start == expectedStart)
    assert(reAlignedAlleles.end == expectedEnd)
    assert(reAlignedAlleles.alleles.length == expectedAlleleString.length)

    for (i <- 0 to expectedAlleleString.length - 1) {
      assert(expectedAlleleString(i) == reAlignedAlleles.alleles(i).getBaseString)
    }
  }

  test("test realignAlleles") {
    testRealignAlleles(
      vtTestReference,
      "20",
      71,
      73,
      Seq("AA", "AAAA", "AAAAAA"),
      67,
      68,
      Seq("T", "TAA", "TAAAA")
    )

    testRealignAlleles(
      vtTestReference,
      "20",
      36,
      76,
      Seq("GAAGGCATAGCCATTACCTTTTAAAAAATTTTAAAAAAAGA", "GA"),
      28,
      67,
      Seq("AAAAAAAAGAAGGCATAGCCATTACCTTTTAAAAAATTTT", "A")
    )

    testRealignAlleles(
      vtTestReference,
      "20",
      1,
      2,
      Seq("GG", "GA"),
      2,
      2,
      Seq("G", "A")
    )

    testRealignAlleles(
      vtTestReference,
      "20",
      1,
      2,
      Seq("GG", "TA"),
      1,
      2,
      Seq("GG", "TA")
    )
  }
}
