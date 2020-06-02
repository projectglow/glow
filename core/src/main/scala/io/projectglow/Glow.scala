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

package io.projectglow

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, SQLUtils, SparkSession}

import io.projectglow.common.Named
import io.projectglow.sql.{GlowSQLExtensions, SqlExtensionProvider}
import io.projectglow.transformers.util.{SnakeCaseMap, StringUtils}

/**
 * The entry point for all language specific functionality, meaning methods that cannot be expressed
 * as SparkSQL expressions.
 *
 * We should expose as little functionality as is necessary through this object and should prefer
 * generic methods with stringly-typed arguments to reduce language-specific maintenance burden.
 */
class GlowBase {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def register(spark: SparkSession): Unit = {
    new GlowSQLExtensions().apply(SQLUtils.getSessionExtensions(spark))
    SqlExtensionProvider.registerFunctions(
      spark.sessionState.conf,
      spark.sessionState.functionRegistry)
  }

  /**
   * Apply a named transformation to a DataFrame of genomic data. All parameters apart from the
   * input data and its schema are provided through the case-insensitive options map.
   *
   * There are no bounds on what the transformer may do. For instance, it's legal for the
   * transformer to materialize the input DataFrame.
   *
   * @return The transformed DataFrame
   */
  def transform(operationName: String, df: DataFrame, options: Map[String, Any]): DataFrame = {
    val stringValuedMap = options.mapValues {
      case s: String => s
      case v => mapper.writeValueAsString(v)
    }.map(identity) // output of mapValues is not serializable: https://github.com/scala/bug/issues/7005
    lookupTransformer(operationName) match {
      case Some(transformer) => transformer.transform(df, new SnakeCaseMap(stringValuedMap))
      case None =>
        throw new IllegalArgumentException(s"No transformer with name $operationName")
    }
  }

  def transform(operationName: String, df: DataFrame, options: (String, Any)*): DataFrame = {
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
    transformerLoader
      .iterator()
      .asScala
      .find(n => StringUtils.toSnakeCase(n.name) == StringUtils.toSnakeCase(name))
  }

  private val transformerLoader = ServiceLoader
    .load(classOf[DataFrameTransformer])
}

object Glow extends GlowBase

trait DataFrameTransformer extends Named {
  def transform(df: DataFrame, options: Map[String, String]): DataFrame
}
