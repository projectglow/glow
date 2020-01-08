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

import htsjdk.samtools.ValidationStringency
import org.apache.spark.sql.DataFrame

import io.projectglow.DataFrameTransformer
import io.projectglow.common.logging.{HlsEventRecorder, HlsTagValues}
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
class NormalizeVariantsTransformer extends DataFrameTransformer {

  override def name: String = "normalize_variants"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {

    import NormalizeVariantsTransformer._

    val validationStringency: ValidationStringency = VCFOptionParser
      .getValidationStringency(options)

    options.get(MODE_KEY).map(StringUtils.toSnakeCase) match {

      case Some(MODE_SPLIT) =>
        // record variantnormalizer event along with its mode
        recordHlsEvent(HlsTagValues.EVENT_NORMALIZE_VARIANTS, Map(MODE_KEY -> MODE_SPLIT))
        VariantNormalizer.normalize(
          df,
          None,
          validationStringency,
          false,
          true
        )

      case Some(MODE_SPLIT_NORMALIZE) =>
        // record variantnormalizer event along with its mode
        recordHlsEvent(HlsTagValues.EVENT_NORMALIZE_VARIANTS, Map(MODE_KEY -> MODE_SPLIT_NORMALIZE))
        VariantNormalizer.normalize(
          df,
          options.get(REFERENCE_GENOME_PATH),
          validationStringency,
          true,
          true
        )

      case Some(MODE_NORMALIZE) | None =>
        // record variantnormalizer event along with its mode
        recordHlsEvent(HlsTagValues.EVENT_NORMALIZE_VARIANTS, Map(MODE_KEY -> MODE_NORMALIZE))
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

object NormalizeVariantsTransformer extends HlsEventRecorder {
  val MODE_KEY = "mode"
  val MODE_NORMALIZE = "normalize"
  val MODE_SPLIT_NORMALIZE = "split_and_normalize"
  val MODE_SPLIT = "split"
  private val REFERENCE_GENOME_PATH = "reference_genome_path"
}
