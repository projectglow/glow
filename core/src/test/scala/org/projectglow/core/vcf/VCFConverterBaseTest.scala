package org.projectglow.core.vcf

import scala.reflect.runtime.universe._

import org.bdgenomics.adam.util.PhredUtils

import org.projectglow.core.common.{GenotypeFields, TestUtils, VCFRow}

trait VCFConverterBaseTest extends TestUtils {

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
    phased = None,
    calls = None,
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
        throw t
    }
  }

  protected def getClassFields[T: TypeTag]: Seq[String] = {
    typeOf[T].members.sorted.collect {
      case m: MethodSymbol if m.isParamAccessor => m.name.toString
    }
  }
}
