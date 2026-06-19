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

package io.projectglow.sql

import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Add, Literal => CatalystLiteral}
import org.apache.spark.sql.functions.{col, lit}

/**
 * Spark 4 only. Locks in the supported-node contract of SQLUtilsShim.columnToExpr:
 * the inverse-of-exprToColumn path (ExpressionColumnNode), the UnresolvedAttribute and
 * Literal node paths that real call sites produce, and the explicit failure for any
 * unsupported ColumnNode so a future call site fails loudly rather than silently wrong.
 */
class SQLUtilsShimSuite extends GlowBaseTest {

  test("exprToColumn -> columnToExpr round-trips the wrapped Expression") {
    val expr = CatalystLiteral(42)
    val roundTripped = SQLUtils.columnToExpr(SQLUtils.exprToColumn(expr))
    assert(roundTripped == expr)
  }

  test("columnToExpr handles a literal column (Literal node)") {
    val expr = SQLUtils.columnToExpr(lit(7))
    assert(expr.isInstanceOf[CatalystLiteral])
    assert(expr.asInstanceOf[CatalystLiteral].value == 7)
  }

  test("columnToExpr handles an attribute column (UnresolvedAttribute node)") {
    val expr = SQLUtils.columnToExpr(col("contigName"))
    assert(expr.isInstanceOf[UnresolvedAttribute])
    assert(expr.asInstanceOf[UnresolvedAttribute].name == "contigName")
  }

  test("exprToColumn -> columnToExpr preserves a composite Expression") {
    val expr = Add(CatalystLiteral(1), CatalystLiteral(2))
    val roundTripped = SQLUtils.columnToExpr(SQLUtils.exprToColumn(expr))
    assert(roundTripped == expr)
  }

  test("columnToExpr throws on an unsupported ColumnNode") {
    // col("a") + col("b") produces a binary-op node that is none of the supported types.
    val e = intercept[IllegalArgumentException] {
      SQLUtils.columnToExpr(col("a") + col("b"))
    }
    assert(e.getMessage.contains("Cannot extract Expression"))
  }
}
