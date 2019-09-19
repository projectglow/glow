package com.databricks.hls.transformers.pipe

import org.apache.spark.sql.DataFrame

import com.databricks.hls.DataFrameTransformer

class CleanupPipeTransformer extends DataFrameTransformer {
  override def name: String = "pipe_cleanup"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {
    Piper.clearCache()
    df
  }
}
