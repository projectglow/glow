package org.apache.spark.sql

import org.apache.spark.sql.types.StructField

import com.databricks.hls.sql.optimizer.WholestageCodegenBoundaryPlan

object SQLUtils {
  def wholestageCodegenBoundary(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    Dataset.ofRows(spark, WholestageCodegenBoundaryPlan(df.logicalPlan))
  }

  def structFieldsEqualExceptNullability(f1: StructField, f2: StructField): Boolean = {
    f1.name == f2.name && f1.dataType.asNullable == f2.dataType.asNullable
  }
}
