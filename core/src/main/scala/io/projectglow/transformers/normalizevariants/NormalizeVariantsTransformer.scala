package io.projectglow.transformers.normalizevariants

import htsjdk.samtools.ValidationStringency
import org.apache.spark.sql.DataFrame

import io.projectglow.DataFrameTransformer
import io.projectglow.common.logging.{HlsMetricDefinitions, HlsTagDefinitions, HlsTagValues, HlsUsageLogging}
import io.projectglow.transformers.util.StringUtils
import io.projectglow.vcf.VCFOptionParser

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
class NormalizeVariantsTransformer extends DataFrameTransformer with HlsUsageLogging {

  override def name: String = "normalize_variants"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {

    import NormalizeVariantsTransformer._

    val validationStringency: ValidationStringency = VCFOptionParser
      .getValidationStringency(options)

    options.get(MODE_KEY).map(StringUtils.toSnakeCase) match {

      case Some(MODE_SPLIT) =>
        // record variantnormalizer event along with its mode
        logNormalizeVariants(MODE_SPLIT)

        VariantNormalizer.normalize(
          df,
          None,
          validationStringency,
          false,
          true
        )

      case Some(MODE_SPLIT_NORMALIZE) =>
        // record variantnormalizer event along with its mode
        logNormalizeVariants(MODE_SPLIT_NORMALIZE)

        VariantNormalizer.normalize(
          df,
          options.get(REFERENCE_GENOME_PATH),
          validationStringency,
          true,
          true
        )

      case Some(MODE_NORMALIZE) | None =>
        // record variantnormalizer event along with its mode
        logNormalizeVariants(MODE_NORMALIZE)

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

private[projectglow] object NormalizeVariantsTransformer extends HlsUsageLogging {
  private val MODE_KEY = "mode"
  val MODE_NORMALIZE = "normalize"
  val MODE_SPLIT_NORMALIZE = "split_and_normalize"
  val MODE_SPLIT = "split"
  private val REFERENCE_GENOME_PATH = "reference_genome_path"

  def logNormalizeVariants(mode: String): Unit = {
    val logOptions = Map(MODE_KEY -> mode)
    recordHlsUsage(
      HlsMetricDefinitions.EVENT_HLS_USAGE,
      Map(
        HlsTagDefinitions.TAG_EVENT_TYPE -> HlsTagValues.EVENT_NORMALIZE_VARIANTS
      ),
      blob = hlsJsonBuilder(logOptions)
    )
  }
}
