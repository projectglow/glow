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

package io.projectglow.transformers.splitmultiallelics

import io.projectglow.common.GlowLogging
import io.projectglow.common.VariantSchemas._
import io.projectglow.sql.GlowBaseTest
import io.projectglow.transformers.splitmultiallelics.VariantSplitter._
import org.apache.commons.math3.util.CombinatoricsUtils
import org.apache.spark.sql.functions._

class VariantSplitterSuite extends GlowBaseTest with GlowLogging {

  lazy val sourceName: String = "vcf"
  lazy val testFolder: String = s"$testDataHome/variantsplitternormalizer-test"

  lazy val vtTestVcfMultiAllelic =
    s"$testFolder/01_IN_altered_multiallelic.vcf"

  lazy val vtTestVcfMultiAllelicExpectedSplit =
    s"$testFolder/01_IN_altered_multiallelic_vtdecompose.vcf"

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
      .foreach { case (rowExp, rowSplit) =>
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
    val dfSplitInfo = splitInfoFields(dfOriginal, None)
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
      .select(
        dfOriginalInfoColumns.head,
        dfOriginalInfoColumns.tail: _*
      ) // make order of columns the same
      .collect
      .zip(
        dfSplitInfo
          .select(
            dfOriginalInfoColumns.head,
            dfOriginalInfoColumns.tail: _*
          ) // make order of columns the same
          .collect)
      .foreach { case (rowExp, rowSplit) =>
        assert(rowExp.equals(rowSplit), s"Expected\n$rowExp\nSplit\n$rowSplit")
      }

    // Check if other columns after splitting are the same as original
    val dfOriginalNonInfoColumns = dfOriginal
      .columns
      .filter(!_.startsWith(infoFieldPrefix))
      .map(name => if (name.contains(".")) s"`${name}`" else name)

    dfOriginal
      .select(
        dfOriginalNonInfoColumns.head,
        dfOriginalNonInfoColumns.tail: _*
      ) // make order of columns the same
      .collect
      .zip(
        dfSplitInfo
          .select(
            dfOriginalNonInfoColumns.head,
            dfOriginalNonInfoColumns.tail: _*
          ) // make order of columns the same
          .collect)
      .foreach { case (rowOrig, rowSplit) =>
        assert(rowOrig.equals(rowSplit), s"Expected\n$rowOrig\nNormalized\n$rowSplit")
      }
  }

  test("split only specified info fields") {
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
    val dfSplitInfo = splitInfoFields(dfOriginal, Some(Seq("INFO_AF")))
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
      .select(
        dfOriginalInfoColumns.head,
        dfOriginalInfoColumns.tail: _*
      ) // make order of columns the same
      .collect
      .zip(
        dfSplitInfo
          .select(
            dfOriginalInfoColumns.head,
            dfOriginalInfoColumns.tail: _*
          ) // make order of columns the same
          .collect)
      .foreach { case (rowExp, rowSplit) =>
        // Only the specified INFO_AF field should be split
        assert(
          rowExp.get(rowExp.fieldIndex("INFO_AF")) == rowSplit.get(rowSplit.fieldIndex("INFO_AF")))
        val splitAC = rowExp.getAs[Seq[Int]](rowExp.fieldIndex("INFO_AC"))
        val unsplitAC = rowSplit.getAs[Seq[Int]](rowSplit.fieldIndex("INFO_AC"))
        assert(splitAC != unsplitAC || unsplitAC.size == 1)
      }

    // Check that INFO_AC matches original DataFrame
    val acAfterSplitting = dfSplitInfo.groupBy("contigName", "start").agg(first("INFO_AC"))
    val originalAc = dfOriginal.select("contigName", "start", "INFO_AC")
    assert(originalAc.count() > 0)
    assert(acAfterSplitting.except(originalAc).count() == 0)
    assert(originalAc.except(acAfterSplitting).count() == 0)
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
      .foreach { case (rowExp, rowSplit) =>
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
      .foreach { case (rowOrig, rowSplit) =>
        assert(rowOrig.equals(rowSplit), s"Expected\n$rowOrig\nNormalized\n$rowSplit")
      }
  }

  case class ColexOrderTestCase(ploidy: Int, altAlleleIdx: Int, truth: Array[Int])
  test("test refAltColexOrderIdxArray") {
    val cases = Seq(
      ColexOrderTestCase(1, 1, Array(0, 1)),
      ColexOrderTestCase(1, 1, Array(0, 1)),
      ColexOrderTestCase(1, 1, Array(0, 1)),
      ColexOrderTestCase(1, 2, Array(0, 2)),
      ColexOrderTestCase(1, 2, Array(0, 2)),
      ColexOrderTestCase(1, 3, Array(0, 3)),
      ColexOrderTestCase(1, 4, Array(0, 4)),
      ColexOrderTestCase(2, 1, Array(0, 1, 2)),
      ColexOrderTestCase(2, 1, Array(0, 1, 2)),
      ColexOrderTestCase(2, 1, Array(0, 1, 2)),
      ColexOrderTestCase(2, 2, Array(0, 3, 5)),
      ColexOrderTestCase(2, 2, Array(0, 3, 5)),
      ColexOrderTestCase(2, 3, Array(0, 6, 9)),
      ColexOrderTestCase(2, 4, Array(0, 10, 14)),
      ColexOrderTestCase(2, 5, Array(0, 15, 20)),
      ColexOrderTestCase(2, 6, Array(0, 21, 27)),
      ColexOrderTestCase(2, 7, Array(0, 28, 35)),
      ColexOrderTestCase(2, 8, Array(0, 36, 44)),
      ColexOrderTestCase(2, 9, Array(0, 45, 54)),
      ColexOrderTestCase(2, 10, Array(0, 55, 65)),
      ColexOrderTestCase(3, 1, Array(0, 1, 2, 3)),
      ColexOrderTestCase(3, 1, Array(0, 1, 2, 3)),
      ColexOrderTestCase(3, 1, Array(0, 1, 2, 3)),
      ColexOrderTestCase(3, 2, Array(0, 4, 7, 9)),
      ColexOrderTestCase(3, 2, Array(0, 4, 7, 9)),
      ColexOrderTestCase(3, 3, Array(0, 10, 16, 19)),
      ColexOrderTestCase(3, 4, Array(0, 20, 30, 34)),
      ColexOrderTestCase(4, 1, Array(0, 1, 2, 3, 4)),
      ColexOrderTestCase(4, 1, Array(0, 1, 2, 3, 4)),
      ColexOrderTestCase(4, 1, Array(0, 1, 2, 3, 4)),
      ColexOrderTestCase(4, 2, Array(0, 5, 9, 12, 14)),
      ColexOrderTestCase(4, 2, Array(0, 5, 9, 12, 14)),
      ColexOrderTestCase(4, 3, Array(0, 15, 25, 31, 34)),
      ColexOrderTestCase(4, 4, Array(0, 35, 55, 65, 69)),
      ColexOrderTestCase(4, 4, Array(0, 35, 55, 65, 69)),
      ColexOrderTestCase(4, 5, Array(0, 70, 105, 120, 125)),
      ColexOrderTestCase(5, 1, Array(0, 1, 2, 3, 4, 5)),
      ColexOrderTestCase(5, 2, Array(0, 6, 11, 15, 18, 20)),
      ColexOrderTestCase(5, 3, Array(0, 21, 36, 46, 52, 55)),
      ColexOrderTestCase(5, 4, Array(0, 56, 91, 111, 121, 125)),
      ColexOrderTestCase(5, 5, Array(0, 126, 196, 231, 246, 251))
    )
    val df = spark
      .createDataFrame(cases)
      .withColumn(
        "glow",
        expr(
          "transform(array_repeat(0, ploidy + 1), (el, i) -> comb(ploidy + altAlleleIdx, ploidy) - comb(ploidy + altAlleleIdx - i, ploidy - i))")
      )
      .where("glow != truth")
    assert(df.count() == 0)
  }

  case class BinomialTestCase(n: Int, k: Int, coeff: Long)
  test("binomial coefficient function") {
    val cases = Range(0, 45).flatMap { n =>
      Range.inclusive(0, n).map { k =>
        BinomialTestCase(n, k, CombinatoricsUtils.binomialCoefficient(n, k))
      }
    }
    val df = spark
      .createDataFrame(cases)
      .withColumn("glow", expr("comb(n, k)"))
      .where("glow != coeff")
    assert(df.count() == 0)
  }
}
