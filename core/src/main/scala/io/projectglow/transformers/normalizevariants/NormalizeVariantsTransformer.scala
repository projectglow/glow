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

import io.projectglow.DataFrameTransformer
import io.projectglow.common.logging.{HlsEventRecorder, HlsTagValues}
import io.projectglow.sql.expressions.VariantNormalizer
import io.projectglow.transformers.util.StringUtils
import org.apache.spark.sql.DataFrame
import io.projectglow.transformers.splitmultiallelics.VariantSplitter

/**
 * Implements DataFrameTransformer to transform the input DataFrame of varaints to an output
 * DataFrame of normalized variants (normalization is as defined in bcftools norm).
 *
 * A path to reference genome containing .fasta, .fasta.fai, and .dict files must be provided
 * through the referenceGenomePath option.
 */
class NormalizeVariantsTransformer extends DataFrameTransformer with HlsEventRecorder {

  import NormalizeVariantsTransformer._

  override def name: String = "normalize_variants"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {

    val refGenomePathString = options.get(REFERENCE_GENOME_PATH)

    if (refGenomePathString.isEmpty) {
      throw new IllegalArgumentException("Reference genome path not provided!")
    }

    if (options.contains(MODE_KEY)) {

      val modeOption = options.get(MODE_KEY).map(StringUtils.toSnakeCase)

      backwardCompatibleTransform(df, refGenomePathString.get, modeOption)
    } else {
      recordHlsEvent(HlsTagValues.EVENT_NORMALIZE_VARIANTS)
      VariantNormalizer.normalize(
        df,
        refGenomePathString.get
      )
    }
  }

  /**
   * The following function is for backward compatibility to the previous API where
   * the normalizer could act in different modes: The default mode was normalizing the variants without splitting
   * multiallelic ones. The "mode" option could be used to change this behavior. Setting "mode" to "split" only splits
   * multiallelic variants and skips normalization. Setting "mode" to split_and_normalize splits multiallelic variants
   * followed by normalization.
   */
  def backwardCompatibleTransform(df: DataFrame, refGenomePathString: String, modeOption: Option[String]): DataFrame = {

    modeOption match {

      case Some(MODE_NORMALIZE) =>
        recordHlsEvent(HlsTagValues.EVENT_NORMALIZE_VARIANTS)
        VariantNormalizer.normalize(
          df,
          refGenomePathString
        )

      case Some(MODE_SPLIT) =>

        // TODO: Log splitter usage
        VariantSplitter.splitVariants(df)

      case Some(MODE_SPLIT_NORMALIZE) =>

        // TODO: Log splitter usage
        VariantSplitter.splitVariants(df)

        recordHlsEvent(HlsTagValues.EVENT_NORMALIZE_VARIANTS)

        VariantNormalizer.normalize(
          df,
          refGenomePathString
        )

      case _ =>
        throw new IllegalArgumentException("Invalid mode option!")
    }
  }
}

object NormalizeVariantsTransformer {

  @deprecated(
    "normalize_variants transformer is now for normalization only. split_multiallelics transformer should be used separately for splitting multiallelics")
  val MODE_KEY = "mode"

  @deprecated(
    "normalize_variants transformer is now for normalization only. split_multiallelics transformer should be used separately for splitting multiallelics")
  val MODE_NORMALIZE = "normalize"

  @deprecated(
    "normalize_variants transformer is now for normalization only. split_multiallelics transformer should be used separately for splitting multiallelics")
  val MODE_SPLIT_NORMALIZE = "split_and_normalize"

  @deprecated(
    "normalize_variants transformer is now for normalization only. split_multiallelics transformer should be used separately for splitting multiallelics")
  val MODE_SPLIT = "split"

  private val REFERENCE_GENOME_PATH = "reference_genome_path"

}
