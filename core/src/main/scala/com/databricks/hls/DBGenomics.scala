package com.databricks.hls

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

import com.databricks.hls.common.Named

/**
 * The entry point for all language specific functionality, meaning methods that cannot be expressed
 * as SparkSQL expressions.
 *
 * We should expose as little functionality as is necessary through this object and should prefer
 * generic methods with stringly-typed arguments to reduce language-specific maintenance burden.
 */
object DBGenomics {

  /**
   * Apply a named transformation to a DataFrame of genomic data. All parameters apart from the
   * input data and its schema are provided through the case-insensitive options map.
   *
   * There are no bounds on what the transformer may do. For instance, it's legal for the
   * transformer to materialize the input DataFrame.
   *
   * @return The transformed DataFrame
   */
  def transform(operationName: String, df: DataFrame, options: Map[String, String]): DataFrame = {
    lookupTransformer(operationName) match {
      case Some(transformer) => transformer.transform(df, CaseInsensitiveMap(options))
      case None =>
        throw new IllegalArgumentException(s"No transformer with name $operationName")
    }
  }

  def transform(operationName: String, df: DataFrame, options: (String, String)*): DataFrame = {
    transform(operationName, df, options.toMap)
  }

  def transform(
      operationName: String,
      df: DataFrame,
      options: java.util.Map[String, String]): DataFrame = {
    transform(operationName, df, options.asScala.toMap)
  }

  private def lookupTransformer(name: String): Option[DataFrameTransformer] = synchronized {
    transformerLoader.reload()
    transformerLoader.iterator().asScala.find(_.name == name)
  }

  private val transformerLoader = ServiceLoader
    .load(classOf[DataFrameTransformer])
}

trait DataFrameTransformer extends Named {
  def transform(df: DataFrame, options: Map[String, String]): DataFrame
}
