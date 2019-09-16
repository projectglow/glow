package com.databricks.hls.transformers.pipe

import org.apache.spark.sql.DataFrame

import com.databricks.hls.DataFrameTransformer

class CleanupPipeTransormer extends DataFrameTransformer {
  override def name: String = "pipecleanup"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {
    Piper.clearCache()
    df
  }
}
