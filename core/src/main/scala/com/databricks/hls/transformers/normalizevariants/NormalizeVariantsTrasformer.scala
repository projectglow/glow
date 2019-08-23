package com.databricks.hls.transformers.normalizevariants

import com.databricks.hls.DataFrameTransformer
import com.databricks.hls.common.HLSLogging
import com.databricks.vcf._
import htsjdk.samtools.ValidationStringency
import org.apache.spark.sql.DataFrame

/**
 * Implements DataFrameTransformer to transform the input DataFrame of varaints to an output
 * DataFrame of normalized variants (normalization is as defined in bcftools norm); Optionally
 * splits multi-allelic variants to bi-allelic (split logic is the one used by gatk).
 *
 * The normalizer can act in different modes:
 * The default mode is normalizing the variants without splitting multi-allelic one.
 * The "mode" option can be used to change this behavior. Setting "mode" to onlysplit only splits
 * multi-allelic variants and skips normalization.
 * Setting "mode" option to splitandnormalize splits multi-allelic variants followed by
 * normalization.
 *
 * A path to reference genome containing .fasta, .fasta.fai, and .dict files must be provided
 * through the referenceGenomePath option.
 */
class NormalizeVariantsTransformer extends DataFrameTransformer with HLSLogging {
  override def name: String = "normalizevariants"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {

    import NormalizeVariantsTransformer._

    val validationStringency: ValidationStringency = VCFOptionParser
      .getValidationStringency(options)

    options.get(MODE_KEY) match {

      case Some(MODE_SPLIT) =>
        VariantNormalizer.normalize(
          df,
          None,
          validationStringency,
          false,
          true
        )

      case Some(MODE_SPLIT_NORMALIZE) =>
        VariantNormalizer.normalize(
          df,
          options.get(REFERENCE_GENOME_PATH),
          validationStringency,
          true,
          true
        )

      case Some(MODE_NORMALIZE) | None =>
        VariantNormalizer.normalize(
          df,
          options.get(REFERENCE_GENOME_PATH),
          validationStringency,
          true,
          false
        )

      case _ =>
        throw new IllegalArgumentException("Invalid mode option!")
    }

  }
}

object NormalizeVariantsTransformer {
  private val MODE_KEY = "mode"
  private val MODE_NORMALIZE = "normalize"
  private val MODE_SPLIT_NORMALIZE = "splitandnormalize"
  private val MODE_SPLIT = "split"
  private val REFERENCE_GENOME_PATH = "referenceGenomePath"

}
