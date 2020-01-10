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

import io.projectglow.common.GlowLogging
import io.projectglow.common.VariantSchemas._
import io.projectglow.sql.GlowBaseTest
import io.projectglow.transformers.normalizevariants.VariantSplitter._
import org.apache.spark.sql.functions._

class VariantSplitterSuite extends GlowBaseTest with GlowLogging {

  lazy val sourceName: String = "vcf"
  lazy val testFolder: String = s"$testDataHome/variantnormalizer-test"

  lazy val vtTestVcfMultiAllelic =
    s"$testFolder/01_IN_altered_multiallelic_new.vcf"

  lazy val vtTestVcfMultiAllelicExpectedSplit =
    s"$testFolder/01_IN_altered_multiallelic_new_vtdecompose.vcf"

  test("test splitVariants") {

    // Read the test vcf
    val dfOriginal = spark
      .read
      .format(sourceName)
      .load(vtTestVcfMultiAllelic)
      .orderBy("contigName", "start", "end")

    // apply splitVariants
    val dfSplitVariants = splitVariants(dfOriginal)
      .orderBy("contigName", "start", "end")

    // read the vt decomposed version of test vcf
    val dfExpected = spark
      .read
      .format(sourceName)
      .load(vtTestVcfMultiAllelicExpectedSplit)
      .orderBy("contigName", "start", "end")

    assert(dfSplitVariants.count() == dfExpected.count())

    val dfSplitVariantsColumns = dfSplitVariants
      .columns
      .filter(f => f != splitFromMultiAllelicField.name)
      .map(name => if (name.contains(".")) s"`${name}`" else name)

    // Check if genotypes column after splitting is the same as vt decompose

    assert(dfSplitVariantsColumns.length == dfExpected.columns.length - 1)

    dfExpected
      .select(dfSplitVariantsColumns.head, dfSplitVariantsColumns.tail: _*)
      .collect
      .zip(
        dfSplitVariants
          .select(dfSplitVariantsColumns.head, dfSplitVariantsColumns.tail: _*)
          .collect)
      .foreach {
        case (rowExp, rowSplit) =>
          assert(rowExp.equals(rowSplit), s"Expected\n$rowExp\nSplit\n$rowSplit")
      }
  }

  test("test splitInfoFields") {

    // Read the test vcf and posexplode
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

    // apply splitInfoFields
    val dfSplitInfo = splitInfoFields(dfOriginal)
      .orderBy("contigName", "start", "end")

    // read the vt decomposed version of test vcf
    val dfExpected = spark
      .read
      .format(sourceName)
      .load(vtTestVcfMultiAllelicExpectedSplit)
      .orderBy("contigName", "start", "end")

    assert(dfSplitInfo.count() == dfExpected.count())
    assert(dfSplitInfo.count() == dfOriginal.count())

    // Check if Info Columns after splitting are the same as vt decompose
    val dfOriginalInfoColumns = dfOriginal
      .columns
      .filter(_.startsWith(infoFieldPrefix))
      .map(name => if (name.contains(".")) s"`${name}`" else name)

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

    // Check if other columns after splitting are the same as original
    val dfOriginalNonInfoColumns = dfOriginal
      .columns
      .filter(!_.startsWith(infoFieldPrefix))
      .map(name => if (name.contains(".")) s"`${name}`" else name)

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

  test("test splitGenotypeFields") {

    // Read the test vcf and posexplode
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

    // apply splitGenotypeFields
    val dfSplitGenotype = splitGenotypeFields(dfOriginal)
      .orderBy("contigName", "start", "end")

    // read the vt decomposed version of test vcf
    val dfExpected = spark
      .read
      .format(sourceName)
      .load(vtTestVcfMultiAllelicExpectedSplit)
      .orderBy("contigName", "start", "end")

    assert(dfSplitGenotype.count() == dfExpected.count())
    assert(dfSplitGenotype.count() == dfOriginal.count())

    // Check if genotypes column after splitting is the same as vt decompose
    dfExpected
      .select(genotypesFieldName)
      .collect
      .zip(
        dfSplitGenotype
          .select(genotypesFieldName)
          .collect)
      .foreach {
        case (rowExp, rowSplit) =>
          assert(rowExp.equals(rowSplit), s"Expected\n$rowExp\nSplit\n$rowSplit")
      }

    // Check if other columns after splitting are the same as original
    val dfOriginalNonInfoColumns = dfOriginal
      .columns
      .filter(!_.startsWith(infoFieldPrefix))
      .map(name => if (name.contains(".")) s"`${name}`" else name)

    dfOriginal
      .drop(genotypesFieldName) // make order of columns the same
      .collect
      .zip(
        dfSplitGenotype
          .drop(genotypesFieldName)
          .collect)
      .foreach {
        case (rowOrig, rowSplit) =>
          assert(rowOrig.equals(rowSplit), s"Expected\n$rowOrig\nNormalized\n$rowSplit")
      }
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

  test("test refAltColexOrderIdxArray") {
    testRefAltColexOrderIdxArray(1, 2, 1, Array())
    testRefAltColexOrderIdxArray(2, 0, 1, Array())
    testRefAltColexOrderIdxArray(2, 2, 0, Array())
    testRefAltColexOrderIdxArray(2, 2, 2, Array())

    testRefAltColexOrderIdxArray(2, 1, 1, Array(0, 1))
    testRefAltColexOrderIdxArray(3, 1, 1, Array(0, 1))
    testRefAltColexOrderIdxArray(4, 1, 1, Array(0, 1))

    testRefAltColexOrderIdxArray(3, 1, 2, Array(0, 2))
    testRefAltColexOrderIdxArray(4, 1, 2, Array(0, 2))

    testRefAltColexOrderIdxArray(4, 1, 3, Array(0, 3))

    testRefAltColexOrderIdxArray(2, 2, 1, Array(0, 1, 2))
    testRefAltColexOrderIdxArray(3, 2, 1, Array(0, 1, 2))
    testRefAltColexOrderIdxArray(4, 2, 1, Array(0, 1, 2))

    testRefAltColexOrderIdxArray(3, 2, 2, Array(0, 3, 5))
    testRefAltColexOrderIdxArray(4, 2, 2, Array(0, 3, 5))

    testRefAltColexOrderIdxArray(4, 2, 3, Array(0, 6, 9))

    testRefAltColexOrderIdxArray(2, 3, 1, Array(0, 1, 2, 3))
    testRefAltColexOrderIdxArray(3, 3, 1, Array(0, 1, 2, 3))
    testRefAltColexOrderIdxArray(4, 3, 1, Array(0, 1, 2, 3))

    testRefAltColexOrderIdxArray(3, 3, 2, Array(0, 4, 7, 9))
    testRefAltColexOrderIdxArray(4, 3, 2, Array(0, 4, 7, 9))

    testRefAltColexOrderIdxArray(4, 3, 3, Array(0, 10, 16, 19))

    testRefAltColexOrderIdxArray(2, 4, 1, Array(0, 1, 2, 3, 4))
    testRefAltColexOrderIdxArray(3, 4, 1, Array(0, 1, 2, 3, 4))
    testRefAltColexOrderIdxArray(4, 4, 1, Array(0, 1, 2, 3, 4))

    testRefAltColexOrderIdxArray(3, 4, 2, Array(0, 5, 9, 12, 14))
    testRefAltColexOrderIdxArray(4, 4, 2, Array(0, 5, 9, 12, 14))

    testRefAltColexOrderIdxArray(4, 4, 3, Array(0, 15, 25, 31, 34))

    // test some general cases
    testRefAltColexOrderIdxArray(5, 4, 4, Array(0, 35, 55, 65, 69))
    testRefAltColexOrderIdxArray(6, 4, 4, Array(0, 35, 55, 65, 69))
    testRefAltColexOrderIdxArray(6, 4, 5, Array(0, 70, 105, 120, 125))

    testRefAltColexOrderIdxArray(6, 5, 1, Array(0, 1, 2, 3, 4, 5))
    testRefAltColexOrderIdxArray(6, 5, 2, Array(0, 6, 11, 15, 18, 20))
    testRefAltColexOrderIdxArray(6, 5, 3, Array(0, 21, 36, 46, 52, 55))
    testRefAltColexOrderIdxArray(6, 5, 4, Array(0, 56, 91, 111, 121, 125))
    testRefAltColexOrderIdxArray(6, 5, 5, Array(0, 126, 196, 231, 246, 251))
  }

}
