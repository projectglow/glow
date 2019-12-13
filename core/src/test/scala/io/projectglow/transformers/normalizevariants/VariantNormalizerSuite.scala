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
import io.projectglow.common.GlowLogging
import io.projectglow.common.VariantSchemas._
import io.projectglow.sql.GlowBaseTest
import io.projectglow.transformers.normalizevariants.VariantNormalizer._
import org.apache.spark.sql.functions._
import org.broadinstitute.hellbender.engine.ReferenceDataSource

class VariantNormalizerSuite extends GlowBaseTest with GlowLogging {

  lazy val sourceName: String = "vcf"
  lazy val testFolder: String = s"$testDataHome/variantnormalizer-test"

  lazy val vtTestReference = s"$testFolder/20_altered.fasta"

  lazy val vtTestVcfMultiAllelic =
    s"$testFolder/01_IN_altered_multiallelic_new.vcf"

  lazy val vtTestVcfMultiAllelicExpectedSplit =
    s"$testFolder/01_IN_altered_multiallelic_new_vtdecompose.vcf"

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

  def testRefAltColexOrderIdxArray(
      numAlleles: Int,
      ploidy: Int,
      altAlleleIdx: Int,
      expected: Array[Int]): Unit = {
    if (numAlleles < 2 || ploidy < 1 || altAlleleIdx < 1 || altAlleleIdx > numAlleles - 1) {
      try {
        refAltColexOrderIdxArray(numAlleles, ploidy, altAlleleIdx)
      } catch {
        case _: IllegalArgumentException => succeed
        case _: Throwable => fail()
      }
    } else {
      assert(refAltColexOrderIdxArray(numAlleles, ploidy, altAlleleIdx) === expected)
    }
  }

  test("refAltColexOrderIdxArray") {
    testRefAltColexOrderIdxArray(1, 2, 1, Array())
    testRefAltColexOrderIdxArray(2, 0, 1, Array())
    testRefAltColexOrderIdxArray(2, 2, 0, Array())
    testRefAltColexOrderIdxArray(2, 2, 2, Array())

    testRefAltColexOrderIdxArray(2, 1, 1, Array(0, 1))
    testRefAltColexOrderIdxArray(2, 2, 1, Array(0, 1, 2))
    testRefAltColexOrderIdxArray(2, 3, 1, Array(0, 1, 2, 3))
    testRefAltColexOrderIdxArray(2, 4, 1, Array(0, 1, 2, 3, 4))

    testRefAltColexOrderIdxArray(3, 1, 1, Array(0, 1))
    testRefAltColexOrderIdxArray(3, 2, 1, Array(0, 1, 2))
    testRefAltColexOrderIdxArray(3, 3, 1, Array(0, 1, 2, 3))
    testRefAltColexOrderIdxArray(3, 4, 1, Array(0, 1, 2, 3, 4))

    testRefAltColexOrderIdxArray(3, 1, 2, Array(0, 2))
    testRefAltColexOrderIdxArray(3, 2, 2, Array(0, 3, 5))
    testRefAltColexOrderIdxArray(3, 3, 2, Array(0, 4, 7, 9))
    testRefAltColexOrderIdxArray(3, 4, 2, Array(0, 5, 9, 12, 14))

    testRefAltColexOrderIdxArray(4, 1, 1, Array(0, 1))
    testRefAltColexOrderIdxArray(4, 2, 1, Array(0, 1, 2))
    testRefAltColexOrderIdxArray(4, 3, 1, Array(0, 1, 2, 3))
    testRefAltColexOrderIdxArray(4, 4, 1, Array(0, 1, 2, 3, 4))

    testRefAltColexOrderIdxArray(4, 1, 2, Array(0, 2))
    testRefAltColexOrderIdxArray(4, 2, 2, Array(0, 3, 5))
    testRefAltColexOrderIdxArray(4, 3, 2, Array(0, 4, 7, 9))
    testRefAltColexOrderIdxArray(4, 4, 2, Array(0, 5, 9, 12, 14))

    testRefAltColexOrderIdxArray(4, 1, 3, Array(0, 3))
    testRefAltColexOrderIdxArray(4, 2, 3, Array(0, 6, 9))
    testRefAltColexOrderIdxArray(4, 3, 3, Array(0, 10, 16, 19))
    testRefAltColexOrderIdxArray(4, 4, 3, Array(0, 15, 25, 31, 34))

  }

  test("test splitInfoFields") {

    val dfOriginal = spark
      .read
      .format(sourceName)
      .load(vtTestVcfMultiAllelic)
      .withColumn(
        splitFromMultiAllelicField.name,
        when(size(col(alternateAllelesField.name)) > 1, lit(true)).otherwise(lit(false))
      )
      .select(
        col("*"),
        posexplode(col(alternateAllelesField.name))
          .as(Array(splitAlleleIdxFieldName, splitAllelesFieldName))
      )
      .orderBy("contigName", "start", "end")

    val dfSplitInfo = splitInfoFields(dfOriginal)
      .orderBy("contigName", "start", "end")

    val dfExpected = spark
      .read
      .format(sourceName)
      .load(vtTestVcfMultiAllelicExpectedSplit)
      .orderBy("contigName", "start", "end")

    val dfOriginalInfoColumns = dfOriginal
      .columns
      .filter(_.startsWith(infoFieldPrefix))
      .map(name => if (name.contains(".")) s"`${name}`" else name)

    val dfOriginalNonInfoColumns = dfOriginal
      .columns
      .filter(!_.startsWith(infoFieldPrefix))
      .map(name => if (name.contains(".")) s"`${name}`" else name)

    assert(dfSplitInfo.count() == dfExpected.count())
    assert(dfSplitInfo.count() == dfOriginal.count())

    dfExpected
      .select(dfOriginalInfoColumns.head, dfOriginalInfoColumns.tail: _*) // make order of columns the same
      .collect
      .zip(
        dfSplitInfo
          .select(dfOriginalInfoColumns.head, dfOriginalInfoColumns.tail: _*) // make order of columns the same
          .collect)
      .foreach {
        case (rowExp, rowSplit) =>
          assert(rowExp.equals(rowSplit), s"Expected\n$rowExp\nSplit\n$rowSplit")
      }

    dfOriginal
      .select(dfOriginalNonInfoColumns.head, dfOriginalNonInfoColumns.tail: _*) // make order of columns the same
      .collect
      .zip(
        dfSplitInfo
          .select(dfOriginalNonInfoColumns.head, dfOriginalNonInfoColumns.tail: _*) // make order of columns the same
          .collect)
      .foreach {
        case (rowOrig, rowSplit) =>
          assert(rowOrig.equals(rowSplit), s"Expected\n$rowOrig\nNormalized\n$rowSplit")
      }

  }

}
