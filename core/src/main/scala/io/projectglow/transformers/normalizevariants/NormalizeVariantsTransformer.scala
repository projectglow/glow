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
import io.projectglow.common.VariantSchemas._
import io.projectglow.common.logging.{HlsEventRecorder, HlsTagValues}
import io.projectglow.functions.{expand_struct, normalize_variant, subset_struct}
import io.projectglow.transformers.normalizevariants.VariantNormalizer._
import io.projectglow.transformers.splitmultiallelics.VariantSplitter
import io.projectglow.transformers.util.StringUtils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * Implements DataFrameTransformer to transform the input DataFrame of variants to an output
 * DataFrame of normalized variants (normalization is as defined in vt normalize or bcftools norm).
 *
 * A path to reference genome containing .fasta, and .fai files must be provided
 * through the reference_genome_path option.
 *
 * This transformer adds a normalizationFlag column to the DataFrame, which indicates whether each
 * row was changed, unchanged, or hit an error as a result of normalization.
 *
 * By default the original values in start, end, referenceAllele, and alternateAlleles columns
 * are replaced by their normalized values. The replace_columns option can be set to false to
 * preserve the original columns and add the normalizedStart, normalizedEnd,
 * normalizedReferenceAllele, and normalizedAlternateAlleles columns as new columns to the DataFrame.
 */
class NormalizeVariantsTransformer extends DataFrameTransformer with HlsEventRecorder {

  import NormalizeVariantsTransformer._

  override def name: String = NORMALIZER_TRANSFORMER_NAME

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {

    val refGenomePathString = options.get(REFERENCE_GENOME_PATH)

    val replaceColumns = options.get(REPLACE_COLUMNS).forall(_.toBoolean)

    if (options.contains(MODE_KEY)) {

      val modeOption = options.get(MODE_KEY).map(StringUtils.toSnakeCase)
      backwardCompatibleTransform(df, refGenomePathString, replaceColumns, modeOption)

    } else {

      recordHlsEvent(HlsTagValues.EVENT_NORMALIZE_VARIANTS)

      if (refGenomePathString.isEmpty) {
        throw new IllegalArgumentException("Reference genome path not provided!")
      }

      normalizeVariants(
        df,
        refGenomePathString.get,
        replaceColumns
      )
    }
  }

  /**
   * The following function is for backward compatibility with the previous API where
   * the normalizer could act in different modes: The default mode was normalizing the variants
   * without splitting multiallelic ones. The "mode" option could be used to change this behavior.
   * Setting "mode" to "split" only splits multiallelic variants and skips normalization.
   * Setting "mode" to split_and_normalize splits multiallelic variants and then normalizes the
   * split variants, which is equivalent to using split_multiallelics transformer followed by
   * normalize_variants transformer.
   */
  def backwardCompatibleTransform(
      df: DataFrame,
      refGenomePathString: Option[String],
      replaceColumns: Boolean,
      modeOption: Option[String]): DataFrame = {

    modeOption match {

      case Some(MODE_NORMALIZE) =>
        recordHlsEvent(HlsTagValues.EVENT_NORMALIZE_VARIANTS)

        if (refGenomePathString.isEmpty) {
          throw new IllegalArgumentException("Reference genome path not provided!")
        }

        normalizeVariants(
          df,
          refGenomePathString.get,
          replaceColumns
        )

      case Some(MODE_SPLIT) =>
        // TODO: Log splitter usage
        VariantSplitter.splitVariants(df)

      case Some(MODE_SPLIT_NORMALIZE) =>
        // TODO: Log splitter usage
        VariantSplitter.splitVariants(df)

        recordHlsEvent(HlsTagValues.EVENT_NORMALIZE_VARIANTS)

        if (refGenomePathString.isEmpty) {
          throw new IllegalArgumentException("Reference genome path not provided!")
        }

        normalizeVariants(
          df,
          refGenomePathString.get,
          replaceColumns
        )

      case _ =>
        throw new IllegalArgumentException("Invalid mode option!")
    }
  }
}

object NormalizeVariantsTransformer {

  /**
   * Normalizes the input DataFrame of variants and outputs them as a Dataframe using
   * normalize_variant expression and handles the replace_columns option
   *
   * @param df                   : Input dataframe of variants
   * @param refGenomePathString  : Path to the underlying reference genome of the variants
   * @param replaceColumns       : replace original columns or not
   * @return normalized DataFrame
   */
  def normalizeVariants(
      df: DataFrame,
      refGenomePathString: String,
      replaceColumns: Boolean): DataFrame = {

    val dfNormalized = df.select(
      col("*"),
      expand_struct(
        normalize_variant(
          col(contigNameField.name),
          col(startField.name),
          col(endField.name),
          col(refAlleleField.name),
          col(alternateAllelesField.name),
          refGenomePathString
        )
      )
    )

    val origFields = Seq(
      startField,
      endField,
      refAlleleField,
      alternateAllelesField
    )

    if (replaceColumns) {

      (0 to origFields.length - 1)
        .foldLeft(dfNormalized)(
          (df, i) =>
            df.withColumn(
                origFields(i).name,
                col(s"${normalizationResultFieldName}.${origFields(i).name}")
              )
        )
        .drop(normalizationResultFieldName)

    } else {
      dfNormalized
    }
  }

  val REFERENCE_GENOME_PATH = "reference_genome_path"
  val REPLACE_COLUMNS = "replace_original_columns"
  val NORMALIZER_TRANSFORMER_NAME = "normalize_variants"

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

}
