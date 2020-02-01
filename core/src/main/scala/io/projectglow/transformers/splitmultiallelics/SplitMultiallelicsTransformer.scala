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

package io.projectglow.transformers.splitmultiallelics

import io.projectglow.DataFrameTransformer
import io.projectglow.common.logging.{HlsMetricDefinitions, HlsTagDefinitions, HlsTagValues, HlsUsageLogging}
import org.apache.spark.sql.DataFrame

/**
 * Implements DataFrameTransformer to transform the input DataFrame of variants to an output
 * DataFrame in which multiallelic rows are split to biallelic (split logic is the one used by vt decompose -s).
 */
class SplitMultiallelicsTransformer extends DataFrameTransformer with HlsUsageLogging {

  import SplitMultiallelicsTransformer._

  override def name: String = SPLITTER_TRANSFORMER_NAME

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {

    // TODO: Log splitter usage

    VariantSplitter.splitVariants(df)

  }
}

object SplitMultiallelicsTransformer {
  val SPLITTER_TRANSFORMER_NAME = "split_multiallelics"
}