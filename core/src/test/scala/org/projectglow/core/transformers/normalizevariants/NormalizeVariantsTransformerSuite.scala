package org.projectglow.core.transformers.normalizevariants

import org.apache.spark.SparkConf
import org.projectglow.core.Glow
import org.projectglow.core.common.HLSLogging
import org.projectglow.core.sql.HLSBaseTest

class NormalizeVariantsTransformerSuite extends HLSBaseTest with HLSLogging {

  lazy val sourceName: String = "vcf"
  lazy val testFolder: String = s"$testDataHome/variantnormalizer-test"

  // gatk test file (multiallelic)
  // The base of vcfs and reference in these test files were taken from gatk
  // LeftTrimAndLeftAlign test suite. The reference genome was trimmed to +/-400 bases around
  // each variant to generate a small reference fasta. The vcf variants were modified accordingly.

  lazy val gatkTestReference =
    s"$testFolder/Homo_sapiens_assembly38.20.21_altered.fasta"

  lazy val gatkTestVcf =
    s"$testFolder/test_left_align_hg38_altered.vcf"

  lazy val gatkTestVcfExpectedSplit =
    s"$testFolder/test_left_align_hg38_altered_gatksplit.vcf"

  lazy val gatkTestVcfExpectedNormalized =
    s"$testFolder/test_left_align_hg38_altered_bcftoolsnormalized.vcf"

  lazy val gatkTestVcfExpectedSplitNormalized =
    s"$testFolder/test_left_align_hg38_altered_gatksplit_bcftoolsnormalized.vcf"

  // These files are similar to above but contain symbolic variants.
  lazy val gatkTestVcfSymbolic =
    s"$testFolder/test_left_align_hg38_altered_symbolic.vcf"

  lazy val gatkTestVcfSymbolicExpectedSplit =
    s"$testFolder/test_left_align_hg38_altered_symbolic_gatksplit.vcf"

  lazy val gatkTestVcfSymbolicExpectedNormalized =
    s"$testFolder/test_left_align_hg38_altered_symbolic_bcftoolsnormalized.vcf"

  lazy val gatkTestVcfSymbolicExpectedSplitNormalized =
    s"$testFolder/test_left_align_hg38_altered_symbolic_gatksplit_bcftoolsnormalized.vcf"

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
    s"$testFolder/01_IN_altered_multiallelic_gatksplit.vcf"

  lazy val vtTestVcfMultiAllelicExpectedNormalized =
    s"$testFolder/01_IN_altered_multiallelic_bcftoolsnormalized.vcf"

  lazy val vtTestVcfMultiAllelicExpectedSplitNormalized =
    s"$testFolder/01_IN_altered_multiallelic_gatksplit_bcftoolsnormalized.vcf"

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
      mode: Option[String]
  ): Unit = {

    val options: Map[String, String] = Map() ++ {
        referenceGenome match {
          case Some(r) => Map("referenceGenomePath" -> r)
          case None => Map()
        }
      } ++ {
        mode match {
          case Some(m) => Map("mode" -> m)
          case None => Map()
        }
      }

    val dfOriginal = spark
      .read
      .format(sourceName)
      .load(originalVCFFileName)

    val dfNormalized = Glow
      .transform(
        "normalize_variants",
        dfOriginal,
        options
      )
      .orderBy("contigName", "start", "end")

    dfNormalized.rdd.count()

    val dfExpected = spark
      .read
      .format(sourceName)
      .load(expectedVCFFileName)
      .orderBy("contigName", "start", "end")

    dfExpected.rdd.count()

    assert(dfNormalized.count() == dfExpected.count())
    dfExpected
      .drop("splitFromMultiAllelic")
      .collect
      .zip(dfNormalized.drop("splitFromMultiAllelic").collect)
      .foreach {
        case (rowExp, rowNorm) =>
          assert(rowExp.equals(rowNorm), s"Expected\n$rowExp\nNormalized\n$rowNorm")
      }
  }

  test("normalization transform do-normalize-no-split no-reference") {
    // vcf containing multi-allelic variants
    try {
      testNormalizedvsExpected(vtTestVcfMultiAllelic, vtTestVcfMultiAllelic, None, None)
    } catch {
      case _: IllegalArgumentException => succeed
      case _: Throwable => fail()
    }
  }

  test("normalization transform do-normalize-no-split") {

    testNormalizedvsExpected(
      vtTestVcfBiallelic,
      vtTestVcfBiallelicExpectedNormalized,
      Option(vtTestReference),
      Option("normalize"))

    testNormalizedvsExpected(
      vtTestVcfMultiAllelic,
      vtTestVcfMultiAllelicExpectedNormalized,
      Option(vtTestReference),
      None)

    testNormalizedvsExpected(
      gatkTestVcf,
      gatkTestVcfExpectedNormalized,
      Option(gatkTestReference),
      Option("normalize"))

    testNormalizedvsExpected(
      gatkTestVcfSymbolic,
      gatkTestVcfSymbolicExpectedNormalized,
      Option(gatkTestReference),
      None)

  }

  test("normalization transform no-normalize-do-split") {

    testNormalizedvsExpected(vtTestVcfBiallelic, vtTestVcfBiallelic, None, Option("split"))

    testNormalizedvsExpected(
      vtTestVcfMultiAllelic,
      vtTestVcfMultiAllelicExpectedSplit,
      None,
      Option("split"))

    testNormalizedvsExpected(gatkTestVcf, gatkTestVcfExpectedSplit, None, Option("split"))

    testNormalizedvsExpected(
      gatkTestVcfSymbolic,
      gatkTestVcfSymbolicExpectedSplit,
      Option(gatkTestReference),
      Option("split"))

  }

  test("normalization transform do-normalize-do-split") {

    testNormalizedvsExpected(
      vtTestVcfBiallelic,
      vtTestVcfBiallelicExpectedNormalized,
      Option(vtTestReference),
      Option("splitAndNormalize"))

    testNormalizedvsExpected(
      vtTestVcfMultiAllelic,
      vtTestVcfMultiAllelicExpectedSplitNormalized,
      Option(vtTestReference),
      Option("splitAndNormalize"))

    testNormalizedvsExpected(
      gatkTestVcf,
      gatkTestVcfExpectedSplitNormalized,
      Option(gatkTestReference),
      Option("splitAndNormalize"))

    testNormalizedvsExpected(
      gatkTestVcfSymbolic,
      gatkTestVcfSymbolicExpectedSplitNormalized,
      Option(gatkTestReference),
      Option("splitAndNormalize"))

  }

}
