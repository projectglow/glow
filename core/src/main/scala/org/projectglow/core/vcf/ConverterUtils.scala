package org.projectglow.core.vcf

import htsjdk.variant.variantcontext.GenotypeLikelihoods
import htsjdk.variant.vcf.VCFConstants
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

private[databricks] object ConverterUtils {

  // Parses the attribute in a map as a comma-separated sequence.
  def getFieldAsSeq(map: scala.collection.Map[String, String], attr: String): Seq[String] = {
    map.get(attr) match {
      case Some(s) => s.split(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR).toSeq
      case None => Nil
    }
  }

  // Logic applied for all filters:
  //  If site has passed all filters: PASS
  //  If site has not passed all filters: semicolon-separated list of codes for filters that fail
  //  If filters have not been applied to this site: missing value
  def getFiltersAppliedPassedFailed(
      filters: Seq[String]): (Option[Boolean], Option[Boolean], Seq[String]) = {

    val filtersApplied = Some(filters.nonEmpty)
    val filtersPassed = if (filters.nonEmpty) {
      Some(filters.head == VCFConstants.PASSES_FILTERS_v4)
    } else {
      None
    }
    val filtersFailed = filters.filter(_ != VCFConstants.PASSES_FILTERS_v4)

    (filtersApplied, filtersPassed, filtersFailed)
  }

  // Get this alternate allele's value in a sequence containing one value for each possible allele
  // (including the reference).
  def liftOptAltIdx[A](seq: Seq[A], idxOpt: Option[Int]): Option[A] = {
    idxOpt.flatMap { idx =>
      seq.lift(idx + 1)
    }
  }

  // Get this (ref, alt) allele pair's values in a sequence containing one value for each possible
  // genotype.
  def liftOptGenotypeIdx[A](seq: Seq[A], idxOpt: Option[Int]): Seq[A] = {
    idxOpt match {
      case Some(idx) =>
        GenotypeLikelihoods
          .getPLIndecesOfAlleles(0, idx + 1)
          .flatMap(seq.lift(_))
      case None => Nil
    }
  }

  // Get this alternate allele's value in a sequence containing one value per alternate allele.
  def liftOptIdx[A](seq: Seq[A], idxOpt: Option[Int]): Option[A] = {
    idxOpt.flatMap { idx =>
      seq.lift(idx)
    }
  }

  // Absence of evidence is not evidence of absence, so we never set Some(false) for a flag.
  def getFlag(map: scala.collection.Map[String, String], attr: String): Option[Boolean] = {
    if (map.contains(attr)) {
      Some(true)
    } else {
      None
    }
  }

  def arrayDataToStringList(array: ArrayData): Seq[String] = {
    array.toObjectArray(StringType).map(_.asInstanceOf[UTF8String].toString)
  }
}
