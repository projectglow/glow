package org.projectglow.core.vcf

import scala.collection.JavaConverters._

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.{VariantContext => HtsjdkVariantContext}
import htsjdk.variant.vcf.VCFHeader
import org.projectglow.core.common.VCFRow

/**
 * VCFRow -> HTSJDK VariantContext
 * Under the hood, this class relies on a [[InternalRowToVariantContextConverter]].
 */
class VCFRowToVariantContextConverter(
    vcfHeader: VCFHeader,
    stringency: ValidationStringency = ValidationStringency.LENIENT)
    extends Serializable {

  // Encoders are not thread safe, so make a copy here
  private val vcfRowEncoder = VCFRow.encoder.copy()
  private val internalRowConverter =
    new InternalRowToVariantContextConverter(
      VCFRow.schema,
      vcfHeader.getMetaDataInInputOrder.asScala.toSet,
      stringency)

  def convert(vcfRow: VCFRow): HtsjdkVariantContext = {
    internalRowConverter
      .convert(vcfRowEncoder.toRow(vcfRow))
      .getOrElse(throw new IllegalStateException(s"Could not convert VCFRow $vcfRow"))
  }
}
