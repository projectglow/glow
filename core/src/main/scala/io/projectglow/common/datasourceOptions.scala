package io.projectglow.common

object CommonOptions {
  val INCLUDE_SAMPLE_IDS = "includeSampleIds"
}

object VCFOptions {
  // Reader-only options
  val FLATTEN_INFO_FIELDS = "flattenInfoFields"
  val SPLIT_TO_BIALLELIC = "splitToBiallelic"
  val VCF_ROW_SCHEMA = "vcfRowSchema"
  val USE_TABIX_INDEX = "useTabixIndex"
  val USE_FILTER_PARSER = "useFilterParser"

  // Writer-only options
  val COMPRESSION = "compression"

  // Reader and writer options
  val VALIDATION_STRINGENCY = "validationStringency"
}

object BgenOptions {
  val IGNORE_EXTENSION_KEY = "ignoreExtension"
  val USE_INDEX_KEY = "useBgenIndex"
  val SAMPLE_FILE_PATH_OPTION_KEY = "sampleFilePath"
  val SAMPLE_ID_COLUMN_OPTION_KEY = "sampleIdColumn"
  val SAMPLE_ID_COLUMN_OPTION_DEFAULT_VALUE = "ID_2"
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
