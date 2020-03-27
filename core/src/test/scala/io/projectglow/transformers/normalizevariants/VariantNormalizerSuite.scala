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

import htsjdk.samtools.reference.ReferenceSequenceFileFactory
import io.projectglow.common.GlowLogging
import io.projectglow.sql.GlowBaseTest
import io.projectglow.transformers.normalizevariants.VariantNormalizer._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String

class VariantNormalizerSuite extends GlowBaseTest with GlowLogging {

  lazy val sourceName: String = "vcf"
  lazy val testFolder: String = s"$testDataHome/variantsplitternormalizer-test"

  lazy val vtTestReference = s"$testFolder/20_altered.fasta"

  /**
   * Tests normalizeVariant method for given alleles and compares with the provided expected
   * outcome
   */
  def testNormalizeVariant(
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
      expectedChanged: Boolean,
      expectedErrorMessage: Option[String]
  ): Unit = {

    val refGenomeIndexedFasta =
      ReferenceSequenceFileFactory.getReferenceSequenceFile(Paths.get(referenceGenome))

    val normalizedVariant =
      normalizeVariant(
        contigName,
        origStart,
        origEnd,
        origRefAllele,
        origAltAlleles,
        refGenomeIndexedFasta
      )

    assert(
      normalizedVariant ==
      InternalRow(
        expectedStart,
        expectedEnd,
        UTF8String.fromString(expectedRefAllele),
        ArrayData.toArrayData(expectedAltAlleles.map(UTF8String.fromString(_))),
        InternalRow(
          expectedChanged,
          expectedErrorMessage.map(UTF8String.fromString).orNull
        )
      )
    )

  }

  def testNormalizeVariant(
      referenceGenome: String,
      contigName: String,
      origStart: Long,
      origEnd: Long,
      origRefAllele: String,
      origAltAlleles: Array[String],
      expectedErrorMessage: Option[String]
  ): Unit = {

    val refGenomeIndexedFasta =
      ReferenceSequenceFileFactory.getReferenceSequenceFile(Paths.get(referenceGenome))

    val normalizedVariant =
      normalizeVariant(
        contigName,
        origStart,
        origEnd,
        origRefAllele,
        origAltAlleles,
        refGenomeIndexedFasta
      )

    val expectedRow = new GenericInternalRow(5)
    expectedRow.update(
      4,
      InternalRow(
        false,
        expectedErrorMessage.map(UTF8String.fromString).orNull
      )
    )
    assert(normalizedVariant == expectedRow)
  }

  test("test normalizeVariant") {
    testNormalizeVariant(
      vtTestReference,
      "20",
      70,
      72,
      "AA",
      Array("AAAA", "AAAAAA"),
      66,
      67,
      "T",
      Array("TAA", "TAAAA"),
      true,
      None
    )

    testNormalizeVariant(
      vtTestReference,
      "20",
      35,
      76,
      "GAAGGCATAGCCATTACCTTTTAAAAAATTTTAAAAAAAGA",
      Array("GA"),
      27,
      67,
      "AAAAAAAAGAAGGCATAGCCATTACCTTTTAAAAAATTTT",
      Array("A"),
      true,
      None
    )

    testNormalizeVariant(
      vtTestReference,
      "20",
      1,
      2,
      "GG",
      Array("GA"),
      2,
      2,
      "G",
      Array("A"),
      true,
      None
    )

    testNormalizeVariant(
      vtTestReference,
      "20",
      1,
      2,
      "GG",
      Array("TA"),
      1,
      2,
      "GG",
      Array("TA"),
      false,
      None
    )

    testNormalizeVariant(
      vtTestReference,
      "20",
      1,
      2,
      "",
      Array("TA"),
      Some("No REF or ALT alleles found.")
    )

  }

}
