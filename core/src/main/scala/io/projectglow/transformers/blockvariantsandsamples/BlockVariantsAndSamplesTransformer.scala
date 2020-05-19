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

package io.projectglow.transformers.blockvariantsandsamples

import io.projectglow.DataFrameTransformer
import io.projectglow.common.logging.HlsUsageLogging

import org.apache.spark.sql.DataFrame

/**
 * Implements DataFrameTransformer to transform the input DataFrame of variants to Blocked GT
 * DataFrame for WGR use
 */
class BlockVariantsAndSamplesTransformer extends DataFrameTransformer with HlsUsageLogging {

  import BlockVariantsAndSamplesTransformer._

  override def name: String = TRANSFORMER_NAME

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {

    val variantsPerBlock = validateIntegerOption(options, VARIANTS_PER_BLOCK)
    val sampleBlockCount = validateIntegerOption(options, SAMPLE_BLOCK_COUNT)

    VariantSampleBlockMaker.makeVariantAndSampleBlocks(df, variantsPerBlock, sampleBlockCount)
  }
}

object BlockVariantsAndSamplesTransformer {
  val TRANSFORMER_NAME = "block_variants_and_samples"
  val VARIANTS_PER_BLOCK = "variants_per_block"
  val SAMPLE_BLOCK_COUNT = "sample_block_count"

  def validateIntegerOption(options: Map[String, String], optionName: String): Int = {
    try {
      (options.get(optionName).get.toInt)
    } catch {
      case _: Throwable =>
        throw new IllegalArgumentException(
          s"$optionName is not provided or cannot be cast as an integer!"
        )
    }
  }
}
