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
import io.projectglow.functions.normalize_variant
import io.projectglow.transformers.normalizevariants.VariantNormalizer._
import io.projectglow.transformers.splitmultiallelics.VariantSplitter
import io.projectglow.transformers.util.StringUtils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Implements DataFrameTransformer to transform the input DataFrame of variants to an output
 * DataFrame of normalized variants (normalization is as defined in vt normalize or bcftools norm).
 *
 * A path to the reference genome .fasta file must be provided through the reference_genome_path
 * option. The .fasta file must be accompanied with a .fai index file in the same folder.
 *
 * The transformer output columns can be controlled by the replace_columns option:
 *
 * If the replace_columns option is false, the transformer does not touch the original start, end,
 * referenceAllele and alternateAlleles columns. Instead, a StructType column called
 * normalizationResult is added to the DataFrame which contains the normalized start, end,
 * referenceAllele, and alternateAlleles columns as well as the normalizationStatus StructType as
 * the fifth field, which contains the following subfields:
 *    - changed: A boolean field indicating  whether the variant data was changed as a result of
 *      normalization.
 *    - errorMessage: An error message in case the attempt at normalizing the row
 *      hit an error. In this case, the changed field will be set to false. If no errors occur
 *      this field will be null. In case of error, the first four fields in normalizationResult will be null.
 *
 * If replace_columns option is true (default), the transformer replaces the original start, end,
 * referenceAllele, and alternateAlleles columns with the normalized value in case they have
 * changed. Otherwise (in case of no change or an error), the original start, end, referenceAllele,
 * and alternateAlleles are not touched. A StructType normalizationStatus column is added to
 * the DataFrame with the same subfields as above.
 */
class NormalizeVariantsTransformer extends DataFrameTransformer with HlsEventRecorder {

  import NormalizeVariantsTransformer._

  override def name: String = NORMALIZER_TRANSFORMER_NAME

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {

    val refGenomePathString = options.get(REFERENCE_GENOME_PATH)

    val replaceColumns = options.get(REPLACE_COLUMNS).forall(_.toBoolean)

    if (options.contains(MODE_KEY)) {

      val modeOption = options.get(MODE_KEY).map(StringUtils.toSnakeCase)

      backwardCompatibleTransform(
        df,
        refGenomePathString,
        replaceColumns,
        modeOption
      )

    } else {

      recordHlsEvent(HlsTagValues.EVENT_NORMALIZE_VARIANTS)

      validateRefGenomeOption(refGenomePathString)

      normalizeVariants(df, refGenomePathString.get, replaceColumns)
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

        validateRefGenomeOption(refGenomePathString)

        normalizeVariants(df, refGenomePathString.get, replaceColumns)

      case Some(MODE_SPLIT) =>
        // TODO: Log splitter usage
        VariantSplitter.splitVariants(df)

      case Some(MODE_SPLIT_NORMALIZE) =>
        // TODO: Log splitter usage
        VariantSplitter.splitVariants(df)

        recordHlsEvent(HlsTagValues.EVENT_NORMALIZE_VARIANTS)

        validateRefGenomeOption(refGenomePathString)

        normalizeVariants(df, refGenomePathString.get, replaceColumns)

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

    val dfNormalized = df
      .withColumn(
        normalizationResultFieldName,
        normalize_variant(
          col(contigNameField.name),
          col(startField.name),
          col(endField.name),
          col(refAlleleField.name),
          col(alternateAllelesField.name),
          refGenomePathString
        )
      )

    val origFields =
      Seq(startField, endField, refAlleleField, alternateAllelesField)

    if (replaceColumns) {

      (0 to origFields.length - 1)
        .foldLeft(dfNormalized)((df, i) =>
          df.withColumn(
            origFields(i).name,
            when(
              col(s"$normalizationResultFieldName.$normalizationStatusFieldName.$changedFieldName"),
              col(s"$normalizationResultFieldName.${origFields(i).name}")
            ).otherwise(col(origFields(i).name))
          ))
        .withColumn(
          normalizationStatusFieldName,
          col(s"$normalizationResultFieldName.$normalizationStatusFieldName")
        )
        .drop(normalizationResultFieldName)

    } else {

      dfNormalized

    }
  }

  def validateRefGenomeOption(refGenomePathString: Option[String]): Unit = {
    if (refGenomePathString.isEmpty) {
      throw new IllegalArgumentException("Reference genome path not provided!")
    }
  }

  val REFERENCE_GENOME_PATH = "reference_genome_path"
  val REPLACE_COLUMNS = "replace_columns"
  val NORMALIZER_TRANSFORMER_NAME = "normalize_variants"

  @deprecated(
    "normalize_variants transformer is now for normalization only. split_multiallelics transformer should be used separately for splitting multiallelics"
  )
  val MODE_KEY = "mode"

  @deprecated(
    "normalize_variants transformer is now for normalization only. split_multiallelics transformer should be used separately for splitting multiallelics"
  )
  val MODE_NORMALIZE = "normalize"

  @deprecated(
    "normalize_variants transformer is now for normalization only. split_multiallelics transformer should be used separately for splitting multiallelics"
  )
  val MODE_SPLIT_NORMALIZE = "split_and_normalize"

  @deprecated(
    "normalize_variants transformer is now for normalization only. split_multiallelics transformer should be used separately for splitting multiallelics"
  )
  val MODE_SPLIT = "split"

}
