package com.databricks.vcf

import scala.reflect.runtime.universe._

import org.bdgenomics.adam.sql.{
  Variant,
  VariantAnnotation,
  VariantCallingAnnotations,
  VariantContext,
  Genotype => GenotypeProduct
}
import org.bdgenomics.adam.util.PhredUtils

import com.databricks.hls.common.TestUtils._
import com.databricks.hls.sql.HLSBaseTest

trait VCFConverterBaseTest extends HLSBaseTest {

  final lazy val defaultContigName = ""
  final lazy val defaultStart = 0L
  final lazy val defaultEnd = 0L
  final lazy val defaultNames = Seq.empty
  final lazy val defaultReferenceAllele = ""
  final lazy val defaultAlternateAlleles = Seq.empty
  final lazy val defaultQual = None

  final val defaultVcfRow = VCFRow(
    contigName = defaultContigName,
    start = defaultStart,
    end = defaultEnd,
    names = defaultNames,
    referenceAllele = defaultReferenceAllele,
    alternateAlleles = defaultAlternateAlleles,
    qual = defaultQual,
    filters = Seq.empty,
    attributes = Map.empty,
    genotypes = Seq(defaultGenotypeFields)
  )

  final lazy val defaultGenotypeFields = GenotypeFields(
    sampleId = None,
    genotype = Some(defaultGenotype),
    depth = None,
    filters = None,
    genotypeLikelihoods = None,
    phredLikelihoods = None,
    posteriorProbabilities = None,
    conditionalQuality = None,
    haplotypeQualities = None,
    expectedAlleleCounts = None,
    mappingQuality = None,
    alleleDepths = None,
    otherFields = Map.empty
  )

  final lazy val defaultGenotype = Genotype(calls = Seq.empty, phased = false)

  final lazy val defaultAlleles = Seq(defaultReferenceAllele) ++ defaultAlternateAlleles


  final lazy val defaultAlternateAllele = None
  final lazy val defaultAlleleIdx = None
  final lazy val defaultNonRefAlleleIdx = None
  final lazy val defaultSplitFromMultiAllelic = false

  protected def phredToLog(p: Int): Double = {
    PhredUtils.phredToLogProbability(p)
  }

  protected def phredToLogFloat(p: Int): Float = {
    PhredUtils.phredToLogProbability(p).toFloat
  }

  // It is ok for f1 to be defined but f2 to be empty.
  private def compareOptionalFloats(f1: Option[Float], f2: Option[Float], field: String): Unit = {

    if (!(f1.isDefined && f2.isEmpty)) {
      try {
        assert(
          f1.isDefined.equals(f2.isDefined),
          "defined %s %s".format(f1.isDefined, f2.isDefined)
        )
        assert(
          f1.getOrElse(0f) ~== f2.getOrElse(0f) relTol 0.2,
          "values %f %f".format(f1.getOrElse(0f), f2.getOrElse(0f))
        )
      } catch {
        case t: Throwable =>
          logger.warn("%s did not match.".format(field))
          throw t
      }
    }
  }

  // It is ok for d1 to be defined but d2 to be empty.
  private def compareOptionalDoubles(
    d1: Option[Double],
    d2: Option[Double],
    field: String): Unit = {

    if (!(d1.isDefined && d2.isEmpty)) {
      try {
        assert(
          d1.isDefined.equals(d2.isDefined),
          "defined %s %s".format(d1.isDefined, d2.isDefined)
        )
        assert(
          d1.getOrElse(0d) ~== d2.getOrElse(0d) relTol 0.2,
          "values %f %f".format(d1.getOrElse(0d), d2.getOrElse(0d))
        )
      } catch {
        case t: Throwable =>
          logger.warn("%s did not match.".format(field))
          throw t
      }
    }
  }

  private def compareSeqFloats(sf1: Seq[Float], sf2: Seq[Float], field: String): Unit = {
    try {
      assert(sf1.length.equals(sf2.length), "length %d %d".format(sf1.length, sf2.length))
      sf1.zip(sf2).foreach {
        case (f1: Float, f2: Float) => assert(f1 ~== f2 relTol 0.2, "values %f %f".format(f1, f2))
      }
    } catch {
      case t: Throwable =>
        logger.warn("%s did not match.".format(field))
        throw t
    }
  }

  private def compareSeqDoubles(sd1: Seq[Double], sd2: Seq[Double], field: String): Unit = {
    try {
      assert(sd1.length.equals(sd2.length), "length %d %d".format(sd1.length, sd2.length))
      sd1.zip(sd2).foreach {
        case (d1: Double, d2: Double) => assert(d1 ~== d2 relTol 0.2, "values %f %f".format(d1, d2))
      }
    } catch {
      case t: Throwable =>
        logger.warn("%s did not match.".format(field))
        throw t
    }
  }

  private def compareStringDoubles(s1: String, s2: String, field: String): Unit = {
    assert(s1.equals(s2) || (s1.toDouble ~== s2.toDouble relTol 0.2), s"$field $s1 $s2")
  }

  // It is ok for keys to be defined in m1 but not in m2.
  private def compareMapStrings(
    m1: scala.collection.Map[String, String],
    m2: scala.collection.Map[String, String],
    field: String): Unit = {

    try {
      assert(m2.keySet.subsetOf(m1.keySet))
      val keySet = m2.keySet
      keySet.foreach { k =>
        compareStringDoubles(m1(k), m2(k), k)
      }
    } catch {
      case t: Throwable =>
        logger.warn("%s did not match.".format(field))
        throw t
    }
  }

  protected def compareVariantAnnotations(va1: VariantAnnotation, va2: VariantAnnotation): Unit = {

    try {
      assert(
        va1.ancestralAllele.equals(va2.ancestralAllele),
        "ancestralAllele %s %s".format(va1.ancestralAllele, va2.ancestralAllele)
      )
      assert(
        va1.alleleCount.equals(va2.alleleCount),
        "alleleCount %s %s".format(va1.alleleCount, va2.alleleCount)
      )
      assert(
        va1.forwardReadDepth.equals(va2.forwardReadDepth),
        "forwardReadDepth %s %s".format(va1.forwardReadDepth, va2.forwardReadDepth)
      )
      assert(
        va1.reverseReadDepth.equals(va2.reverseReadDepth),
        "reverseReadDepth %s %s".format(va1.reverseReadDepth, va2.reverseReadDepth)
      )
      assert(
        va1.referenceReadDepth.equals(va2.referenceReadDepth),
        "referenceReadDepth %s %s".format(va1.referenceReadDepth, va2.referenceReadDepth)
      )
      assert(
        va1.referenceForwardReadDepth.equals(va2.referenceForwardReadDepth),
        "referenceForwardReadDepth %s %s"
          .format(va1.referenceForwardReadDepth, va2.referenceForwardReadDepth)
      )
      assert(
        va1.referenceReverseReadDepth.equals(va2.referenceReverseReadDepth),
        "referenceReverseReadDepth %s %s"
          .format(va1.referenceReverseReadDepth, va2.referenceReverseReadDepth)
      )
      assert(va1.cigar.equals(va2.cigar), "cigar %s %s".format(va1.cigar, va2.cigar))
      assert(va1.dbSnp.equals(va2.dbSnp), "dbSnp %s %s".format(va1.dbSnp, va2.dbSnp))
      assert(va1.hapMap2.equals(va2.hapMap2), "hapMap2 %s %s".format(va1.hapMap2, va2.hapMap2))
      assert(va1.hapMap3.equals(va2.hapMap3), "hapMap3 %s %s".format(va1.hapMap3, va2.hapMap3))
      assert(
        va1.validated.equals(va2.validated),
        "validated %s %s".format(va1.validated, va2.validated)
      )
      assert(
        va1.thousandGenomes.equals(va2.thousandGenomes),
        "thousandGenomes %s %s".format(va1.thousandGenomes, va2.thousandGenomes)
      )
      assert(va1.somatic.equals(va2.somatic), "somatic %s %s".format(va1.somatic, va2.somatic))
      assert(
        va1.transcriptEffects.equals(va2.transcriptEffects),
        "transcriptEffects %s %s".format(va1.transcriptEffects, va2.transcriptEffects)
      )

      compareOptionalFloats(va1.alleleFrequency, va2.alleleFrequency, "alleleFrequency")

      compareMapStrings(va1.attributes, va2.attributes, "attributes")
    } catch {
      case t: Throwable =>
        logger.warn("Variant annotations did not match.")
        logger.warn("Variant annotation 1: %s".format(va1.toAvro))
        logger.warn("Variant annotation 2: %s".format(va2.toAvro))
        throw t
    }
  }

  private def compareOptionalVariantAnnotations(
    va1: Option[VariantAnnotation],
    va2: Option[VariantAnnotation]): Unit = {

    try {
      assert(
        va1.isDefined.equals(va2.isDefined),
        "annotation defined %s %s".format(va1.isDefined, va2.isDefined)
      )
      if (va1.isDefined) {
        compareVariantAnnotations(va1.get, va2.get)
      }
    } catch {
      case t: Throwable =>
        logger.warn("Variant annotations did not match.")
        if (!(va1.isDefined && va2.isEmpty)) throw t
    }
  }

  protected def compareVariantCallingAnnotations(
    vca1: VariantCallingAnnotations,
    vca2: VariantCallingAnnotations): Unit = {

    try {
      // We don't compare genotype filters as the HTSJDK does not parse them, while we do.
      // See https://github.com/samtools/htsjdk/issues/741

      assert(
        vca1.downsampled.equals(vca2.downsampled),
        "downsampled %s %s".format(vca1.downsampled, vca2.downsampled)
      )
      assert(
        vca1.mapq0Reads.equals(vca2.mapq0Reads),
        "mapq0Reads %s %s".format(vca1.mapq0Reads, vca2.mapq0Reads)
      )
      assert(vca1.culprit.equals(vca2.culprit), "culprit %s %s".format(vca1.culprit, vca2.culprit))

      compareOptionalFloats(vca1.baseQRankSum, vca2.baseQRankSum, "baseQRankSum")
      compareOptionalFloats(
        vca1.fisherStrandBiasPValue,
        vca2.fisherStrandBiasPValue,
        "fisherStrandBiasPValue"
      )
      compareOptionalFloats(vca1.rmsMapQ, vca2.rmsMapQ, "rmsMapQ")
      compareOptionalFloats(vca1.mqRankSum, vca2.mqRankSum, "mqRankSum")
      compareOptionalFloats(
        vca1.readPositionRankSum,
        vca2.readPositionRankSum,
        "readPositionRankSum"
      )
      compareOptionalFloats(vca1.vqslod, vca2.vqslod, "vqslod")

      compareSeqFloats(vca1.genotypePriors, vca2.genotypePriors, "genotypePriors")
      compareSeqFloats(vca1.genotypePosteriors, vca2.genotypePosteriors, "genotypePosteriors")

      compareMapStrings(vca1.attributes, vca2.attributes, "attributes")
    } catch {
      case t: Throwable =>
        logger.warn("Variant calling annotations did not match.")
        logger.warn("Variant calling annotation 1: %s".format(vca1.toAvro))
        logger.warn("Variant calling annotation 2: %s".format(vca2.toAvro))
        throw t
    }
  }
}
