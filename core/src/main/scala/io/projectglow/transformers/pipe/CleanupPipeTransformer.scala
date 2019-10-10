package io.projectglow.transformers.pipe

import org.apache.spark.sql.DataFrame

import io.projectglow.DataFrameTransformer

class CleanupPipeTransformer extends DataFrameTransformer {
  override def name: String = "pipe_cleanup"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {
    Piper.clearCache()
    df
  }
}
