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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo

// Spark 3.0 APIs that are not inter-version compatible
object SparkShim extends SparkShimBase {
  // [SPARK-25393][SQL] Adding new function from_csv()
  // Refactors classes from [[org.apache.spark.sql.execution.datasources.csv]] to [[org.apache.spark.sql.catalyst.csv]]
  override type CSVOptions = org.apache.spark.sql.catalyst.csv.CSVOptions
  override type UnivocityParser = org.apache.spark.sql.catalyst.csv.UnivocityParser

  override def wrapUnivocityParse(parser: UnivocityParser)(input: String): Option[InternalRow] = {
    parser.parse(input)
  }

  // [SPARK-27328][SQL] Add 'deprecated' in ExpressionDescription for extended usage and SQL doc
  // Adds 'deprecated' argument to the ExpressionInfo constructor
  override def createExpressionInfo(
      className: String,
      db: String,
      name: String,
      usage: String,
      arguments: String,
      examples: String,
      note: String,
      since: String): ExpressionInfo = {
    // TODO fix this up later.
    new ExpressionInfo(
      className,
      db,
      name,
      usage,
      arguments
    )
  }

  // [SPARK-28077][SQL] Support ANSI SQL OVERLAY function.
  // Adds QuaternaryExpression
  abstract class QuaternaryExpression
      extends org.apache.spark.sql.catalyst.expressions.QuaternaryExpression
}
