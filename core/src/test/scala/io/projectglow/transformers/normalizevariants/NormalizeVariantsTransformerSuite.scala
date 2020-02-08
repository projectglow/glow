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

import io.projectglow.Glow
import io.projectglow.common.{CommonOptions, GlowLogging}
import io.projectglow.sql.GlowBaseTest
import org.apache.spark.SparkConf
import io.projectglow.common.VariantSchemas._
import io.projectglow.transformers.normalizevariants.VariantNormalizer._
import io.projectglow.transformers.normalizevariants.NormalizeVariantsTransformer._
import io.projectglow.transformers.splitmultiallelics.SplitMultiallelicsTransformer._
import io.projectglow.functions.expand_struct

import org.apache.spark.sql.functions.col

class NormalizeVariantsTransformerSuite extends GlowBaseTest with GlowLogging {

  import io.projectglow.transformers.splitmultiallelics
  lazy val sourceName: String = "vcf"
  lazy val testFolder: String = s"$testDataHome/variantsplitternormalizer-test"

  // gatk test file (multiallelic)
  // The base of vcfs and reference in these test files were taken from gatk
  // LeftTrimAndLeftAlign test suite. The reference genome was trimmed to +/-400 bases around
  // each variant to generate a small reference fasta. The vcf variants were modified accordingly.

  lazy val gatkTestReference =
    s"$testFolder/Homo_sapiens_assembly38.20.21_altered.fasta"

  lazy val gatkTestVcf =
    s"$testFolder/test_left_align_hg38_altered.vcf"

  lazy val gatkTestVcfExpectedNormalized =
    s"$testFolder/test_left_align_hg38_altered_bcftoolsnormalized.vcf"

  lazy val gatkTestVcfExpectedSplitNormalized =
    s"$testFolder/test_left_align_hg38_altered_vtdecompose_bcftoolsnormalized.vcf"

  // These files are similar to above but contain symbolic variants.
  lazy val gatkTestVcfSymbolic =
    s"$testFolder/test_left_align_hg38_altered_symbolic.vcf"

  lazy val gatkTestVcfSymbolicExpectedSplit =
    s"$testFolder/test_left_align_hg38_altered_symbolic_vtdecompose.vcf"

  lazy val gatkTestVcfSymbolicExpectedNormalized =
    s"$testFolder/test_left_align_hg38_altered_symbolic_bcftoolsnormalized.vcf"

  lazy val gatkTestVcfSymbolicExpectedSplitNormalized =
    s"$testFolder/test_left_align_hg38_altered_symbolic_vtdecompose_bcftoolsnormalized.vcf"

  // vt test files
  // The base of vcfs and reference in these test files were taken from vt
  // (https://genome.sph.umich.edu/wiki/Vt) normalization test suite. The vcf in this test suite
  // is biallelic. The reference genome was trimmed to +/-100 bases around each variant to
  // generate a small reference fasta. The vcf variants were modified accordingly.
  //
  // The multialleleic versions were generated by artificially adding more alleles and
  // corresponding genotypes to some of the variants.
  lazy val vtTestReference = s"$testFolder/20_altered.fasta"

  lazy val vtTestVcfBiallelic =
    s"$testFolder/01_IN_altered_biallelic.vcf"

  lazy val vtTestVcfBiallelicExpectedNormalized =
    s"$testFolder/01_IN_altered_biallelic_bcftoolsnormalized.vcf"

  lazy val vtTestVcfMultiAllelic =
    s"$testFolder/01_IN_altered_multiallelic.vcf"

  lazy val vtTestVcfMultiAllelicExpectedSplit =
    s"$testFolder/01_IN_altered_multiallelic_vtdecompose.vcf"

  lazy val vtTestVcfMultiAllelicExpectedNormalized =
    s"$testFolder/01_IN_altered_multiallelic_bcftoolsnormalized.vcf"

  lazy val vtTestVcfMultiAllelicExpectedSplitNormalized =
    s"$testFolder/01_IN_altered_multiallelic_vtdecompose_bcftoolsnormalized.vcf"

  override def sparkConf: SparkConf = {
    super
      .sparkConf
      .set(
        "spark.hadoop.io.compression.codecs",
        "org.seqdoop.hadoop_bam.util.BGZFCodec"
      )
  }

  /**
   *  Tests whether the transformed VCF matches the expected VCF
   */
  def testNormalizedvsExpected(
      originalVCFFileName: String,
      expectedVCFFileName: String,
      referenceGenome: Option[String],
      mode: Option[String],
      includeSampleIds: Boolean
  ): Unit = {

    val (modeMap: Map[String, String], split: Boolean) = mode match {
      case Some(m) if (m == MODE_SPLIT_NORMALIZE) => (Map(MODE_KEY -> m), true)
      case Some(m) => (Map(MODE_KEY -> m), false)
      case None => (Map(), false)
    }

    val options: Map[String, String] = Map() ++ {
        referenceGenome match {
          case Some(r) => Map(REFERENCE_GENOME_PATH -> r)
          case None => Map()
        }
      } ++ modeMap

    val dfOriginal = spark
      .read
      .format(sourceName)
      .options(Map(CommonOptions.INCLUDE_SAMPLE_IDS -> includeSampleIds.toString))
      .load(originalVCFFileName)

    val dfNormalized = Glow
      .transform(
        NORMALIZER_TRANSFORMER_NAME,
        if (split) {
          Glow.transform(SPLITTER_TRANSFORMER_NAME, dfOriginal)
        } else {
          dfOriginal
        },
        options
      )
      .orderBy(contigNameField.name, startField.name, endField.name)

    val dfExpected = spark
      .read
      .format(sourceName)
      .options(Map(CommonOptions.INCLUDE_SAMPLE_IDS -> includeSampleIds.toString))
      .load(expectedVCFFileName)
      .orderBy(contigNameField.name, startField.name, endField.name)

    val dfExpectedColumns =
      dfExpected.columns.map(name => s"`${name}`")

    assert(dfNormalized.count() == dfExpected.count())

    dfExpected
      .drop(splitFromMultiAllelicField.name, normalizationStatusFieldName)
      .collect
      .zip(
        dfNormalized
          .select(dfExpectedColumns.head, dfExpectedColumns.tail: _*) // make order of columns the same
          .drop(splitFromMultiAllelicField.name, normalizationStatusFieldName)
          .collect
      )
      .foreach {
        case (rowExp, rowNorm) =>
          assert(rowExp.equals(rowNorm), s"Expected\n$rowExp\nNormalized\n$rowNorm")
      }
  }

  def testNormalizedvsExpected(
      originalVCFFileName: String,
      expectedVCFFileName: String,
      referenceGenome: Option[String],
      includeSampleIds: Boolean
  ): Unit = {
    testNormalizedvsExpected(
      originalVCFFileName,
      expectedVCFFileName,
      referenceGenome,
      None,
      includeSampleIds)
  }

  def testNormalizedvsExpected(
      originalVCFFileName: String,
      expectedVCFFileName: String,
      referenceGenome: Option[String]
  ): Unit = {
    testNormalizedvsExpected(originalVCFFileName, expectedVCFFileName, referenceGenome, None, true)
  }

  def testBackwardCompatibility(
      originalVCFFileName: String,
      expectedVCFFileName: String,
      referenceGenome: Option[String],
      mode: Option[String]
  ): Unit = {
    testNormalizedvsExpected(originalVCFFileName, expectedVCFFileName, referenceGenome, mode, true)
  }

  test("normalization transform no-reference") {
    // vcf containing multi-allelic variants
    try {
      testNormalizedvsExpected(vtTestVcfMultiAllelic, vtTestVcfMultiAllelic, None)
    } catch {
      case _: IllegalArgumentException => succeed
      case _: Throwable => fail()
    }
  }

  test("normalize variants transformer") {

    testNormalizedvsExpected(
      vtTestVcfBiallelic,
      vtTestVcfBiallelicExpectedNormalized,
      Option(vtTestReference)
    )

    testNormalizedvsExpected(
      vtTestVcfMultiAllelic,
      vtTestVcfMultiAllelicExpectedNormalized,
      Option(vtTestReference)
    )

    // without sampleIds
    testNormalizedvsExpected(
      vtTestVcfMultiAllelic,
      vtTestVcfMultiAllelicExpectedNormalized,
      Option(vtTestReference),
      false
    )

    // without sampleIds
    testNormalizedvsExpected(
      vtTestVcfMultiAllelic,
      vtTestVcfMultiAllelicExpectedNormalized,
      Option(vtTestReference),
      None,
      false)

    testNormalizedvsExpected(
      gatkTestVcf,
      gatkTestVcfExpectedNormalized,
      Option(gatkTestReference)
    )

    testNormalizedvsExpected(
      gatkTestVcfSymbolic,
      gatkTestVcfSymbolicExpectedNormalized,
      Option(gatkTestReference)
    )

  }

  test("backward mode option compatibility do-normalize-no-split") {

    testBackwardCompatibility(
      vtTestVcfMultiAllelic,
      vtTestVcfMultiAllelicExpectedNormalized,
      Option(vtTestReference),
      Option(MODE_NORMALIZE)
    )

  }

  test("backward mode option compatibility no-normalize-do-split") {

    val dfOriginal = spark
      .read
      .format(sourceName)
      .options(Map(CommonOptions.INCLUDE_SAMPLE_IDS -> "true"))
      .load(vtTestVcfMultiAllelic)

    val dfSplit = Glow
      .transform(
        SPLITTER_TRANSFORMER_NAME,
        dfOriginal
      )
      .orderBy(contigNameField.name, startField.name, endField.name)

    val dfOldSplit = Glow
      .transform(NORMALIZER_TRANSFORMER_NAME, dfOriginal, Map(MODE_KEY -> MODE_SPLIT))
      .orderBy(contigNameField.name, startField.name, endField.name)

    val dfOldSplitColumns =
      dfOldSplit.columns.map(name => if (name.contains(".")) s"`${name}`" else name)

    assert(dfSplit.count() == dfOldSplit.count())

    dfOldSplit
      .drop(splitFromMultiAllelicField.name)
      .collect
      .zip(
        dfSplit
          .select(dfOldSplitColumns.head, dfOldSplitColumns.tail: _*) // make order of columns the same
          .drop(splitFromMultiAllelicField.name)
          .collect
      )
      .foreach {
        case (rowExp, rowNorm) =>
          assert(rowExp.equals(rowNorm), s"Expected\n$rowExp\nNormalized\n$rowNorm")
      }
  }

  test("backward mode option compatibility do-normalize-do-split") {

    testBackwardCompatibility(
      vtTestVcfBiallelic,
      vtTestVcfBiallelicExpectedNormalized,
      Option(vtTestReference),
      Option(MODE_SPLIT_NORMALIZE)
    )

    testBackwardCompatibility(
      vtTestVcfMultiAllelic,
      vtTestVcfMultiAllelicExpectedSplitNormalized,
      Option(vtTestReference),
      Option(MODE_SPLIT_NORMALIZE)
    )

    testBackwardCompatibility(
      gatkTestVcf,
      gatkTestVcfExpectedSplitNormalized,
      Option(gatkTestReference),
      Option(MODE_SPLIT_NORMALIZE)
    )

    testBackwardCompatibility(
      gatkTestVcfSymbolic,
      gatkTestVcfSymbolicExpectedSplitNormalized,
      Option(gatkTestReference),
      Option(MODE_SPLIT_NORMALIZE)
    )
  }

  test("replace_columns option") {
    val dfOriginal = spark
      .read
      .format(sourceName)
      .options(Map(CommonOptions.INCLUDE_SAMPLE_IDS -> "true"))
      .load(vtTestVcfBiallelic)

    val dfNormalizedReplaced = Glow
      .transform(
        NORMALIZER_TRANSFORMER_NAME,
        dfOriginal,
        Map(REFERENCE_GENOME_PATH -> vtTestReference)
      )
      .select(
        contigNameField.name,
        startField.name,
        endField.name,
        refAlleleField.name,
        alternateAllelesField.name)
      .orderBy(contigNameField.name, startField.name, endField.name)

    val dfNormalizedNonReplaced = Glow
      .transform(
        NORMALIZER_TRANSFORMER_NAME,
        dfOriginal,
        Map(
          REFERENCE_GENOME_PATH -> vtTestReference,
          REPLACE_COLUMNS -> "false"
        )
      )
      .select(
        contigNameField.name,
        s"${normalizationResultFieldName}.${startField.name}",
        s"${normalizationResultFieldName}.${endField.name}",
        s"${normalizationResultFieldName}.${refAlleleField.name}",
        s"${normalizationResultFieldName}.${alternateAllelesField.name}"
      )
      .orderBy(contigNameField.name, startField.name, endField.name)

    assert(dfNormalizedReplaced.count() == dfNormalizedNonReplaced.count())

    dfNormalizedReplaced
      .collect
      .zip(
        dfNormalizedNonReplaced.collect
      )
      .foreach {
        case (rowNormReplaced, rowNormNonReplaced) =>
          assert(
            rowNormReplaced.equals(rowNormNonReplaced),
            s"Repalced\n$rowNormReplaced\nNon-replaced\n$rowNormNonReplaced")
      }
  }

  /*

  test( "dev") {

    val dfOriginal = spark
      .read
      .format(sourceName)
      .load(gatkTestVcfSymbolic)

    val dfNormalized = Glow
      .transform(
        "normalize_variants",
        dfOriginal
        ,
        Map(
          "reference_genome_path" -> gatkTestReference,
          "replace_original_columns" -> "true"
        )
      )// .select("normalizationResult", "normalizationStatus")

    dfNormalized
      .show(false)
  }
 */
}
