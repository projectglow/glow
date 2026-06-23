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

package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.{ExpressionColumnNode, SparkSession => ClassicSparkSession}
import org.apache.spark.sql.internal.{Literal => LiteralNode, UnresolvedAttribute => UnresolvedAttributeNode}
import org.apache.spark.sql.types.StructType

// Spark 4 shim for SQLUtils methods that differ between Spark versions.
// In Spark 4, SparkSession is abstract (use classic.SparkSession).
// Column no longer has .expr or Column(Expression) constructor.
// Use ExpressionColumnNode to bridge between Expression and ColumnNode.
private[sql] object SQLUtilsShim {

  def internalCreateDataFrame(
      sess: SparkSession,
      catalystRows: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean): DataFrame = {
    sess
      .asInstanceOf[ClassicSparkSession]
      .internalCreateDataFrame(catalystRows, schema, isStreaming)
  }

  def getSessionExtensions(session: SparkSession): SparkSessionExtensions = {
    session.asInstanceOf[ClassicSparkSession].extensions
  }

  // Supported-node contract for columnToExpr:
  //
  // In Spark 4, a Column wraps a `ColumnNode` rather than a Catalyst `Expression`.
  // Glow only constructs Columns from the node types below, so those are the only
  // ones this method translates back to Expressions:
  //   - ExpressionColumnNode  -> the wrapped Expression (the inverse of exprToColumn)
  //   - UnresolvedAttribute   -> catalyst UnresolvedAttribute (col("name") / df("name"))
  //   - Literal               -> catalyst Literal (lit(value))
  // Any other node type throws IllegalArgumentException by design: glow expression
  // builders are expected to pass only the nodes above, so an unsupported node
  // indicates a new call site that must be handled here explicitly rather than
  // silently producing a wrong Expression. See SQLUtilsShimSuite for coverage of
  // each supported node type and the failure case.
  def columnToExpr(col: Column): Expression = {
    col.node match {
      case ecn: ExpressionColumnNode => ecn.expression
      case ua: UnresolvedAttributeNode =>
        org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute(ua.nameParts)
      case lit: LiteralNode =>
        val catalystLit = lit.dataType match {
          case Some(dt) =>
            org.apache.spark.sql.catalyst.expressions.Literal.create(lit.value, dt)
          case None =>
            org.apache.spark.sql.catalyst.expressions.Literal(lit.value)
        }
        catalystLit
      case other =>
        throw new IllegalArgumentException(
          s"Cannot extract Expression from Column with node type ${other.getClass.getName}.")
    }
  }

  def exprToColumn(expr: Expression): Column = {
    // In Spark 4, wrap the Expression in an ExpressionColumnNode (a ColumnNode
    // that wraps an Expression), then create a Column from that node.
    Column(ExpressionColumnNode(expr))
  }
}
