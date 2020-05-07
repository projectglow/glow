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

package io.projectglow.common

object CommonOptions {
  val INCLUDE_SAMPLE_IDS = "includeSampleIds"
}

object VCFOptions {
  // Reader-only options
  val FLATTEN_INFO_FIELDS = "flattenInfoFields"
  @deprecated
  val SPLIT_TO_BIALLELIC = "splitToBiallelic"
  val USE_TABIX_INDEX = "useTabixIndex"
  val USE_FILTER_PARSER = "useFilterParser"

  // Writer-only options
  val COMPRESSION = "compression"

  // Reader and writer options
  val VALIDATION_STRINGENCY = "validationStringency"
}

object BgenOptions {
  // Reader options
  val IGNORE_EXTENSION_KEY = "ignoreExtension"
  val USE_INDEX_KEY = "useBgenIndex"
  val SAMPLE_FILE_PATH_OPTION_KEY = "sampleFilePath"
  val SAMPLE_ID_COLUMN_OPTION_KEY = "sampleIdColumn"
  val SAMPLE_ID_COLUMN_OPTION_DEFAULT_VALUE = "ID_2"

  // bigbgen write options
  val BITS_PER_PROB_KEY = "bitsPerProbability"
  val BITS_PER_PROB_DEFAULT_VALUE = "16"

  val MAX_PLOIDY_KEY = "maximumInferredPloidy"
  val MAX_PLOIDY_VALUE = "10"

  val DEFAULT_PLOIDY_KEY = "defaultInferredPloidy"
  val DEFAULT_PLOIDY_VALUE = "2"

  val DEFAULT_PHASING_KEY = "defaultInferredPhasing"
  val DEFAULT_PHASING_VALUE = "false"
}

object PlinkOptions {
  // Delimiter options
  val FAM_DELIMITER_KEY = "famDelimiter"
  val BIM_DELIMITER_KEY = "bimDelimiter"
  val DEFAULT_FAM_DELIMITER_VALUE = " "
  val DEFAULT_BIM_DELIMITER_VALUE = "\t"

  // Sample ID options
  val MERGE_FID_IID = "mergeFidIid"
}
