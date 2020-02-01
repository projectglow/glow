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

import htsjdk.samtools.reference.IndexedFastaSequenceFile
import htsjdk.variant.variantcontext.Allele
import io.projectglow.common.GlowLogging
import io.projectglow.sql.GlowBaseTest
import io.projectglow.transformers.normalizevariants.VariantNormalizer.realignAlleles
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String
import org.broadinstitute.hellbender.engine.ReferenceDataSource

class VariantNormalizerSuite extends GlowBaseTest with GlowLogging {

  lazy val sourceName: String = "vcf"
  lazy val testFolder: String = s"$testDataHome/variantsplitternormalizer-test"

  lazy val vtTestReference = s"$testFolder/20_altered.fasta"


  /**
   * Tests realignAlleles method for given alleles and compares with the provided expected
   * outcome
   */
  def testRealignAlleles(
      referenceGenome: String,
      contigName: String,
      origStart: Long,
      origEnd: Long,
      origRefAllele: String,
      origAltAlleles: Array[String],
      expectedStart: Long,
      expectedEnd: Long,
      expectedRefAllele: String,
      expectedAltAlleles: Array[String],
      expectedFlag: String
      ): Unit = {

    val refGenomeIndexedFasta = new IndexedFastaSequenceFile(Paths.get(referenceGenome))


    val normalizedVariant =
      realignAlleles(
        contigName,
        origStart,
        origEnd,
        origRefAllele,
        origAltAlleles,
        refGenomeIndexedFasta
      )

    assert(normalizedVariant ==
    InternalRow(expectedStart, expectedEnd, UTF8String.fromString(expectedRefAllele), ArrayData.toArrayData(expectedAltAlleles.map(UTF8String.fromString(_))), UTF8String.fromString(expectedFlag))
))
  }

  test("test realignAlleles") {
    testRealignAlleles(
      vtTestReference,
      "20",
      71,
      73,
      "AA",
      Array("AAAA", "AAAAAA"),
      67,
      68,
      "T",
      Array("TAA", "TAAAA"),
      FLAG_CHANGED
    )

      /*
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

       */
  }

}
