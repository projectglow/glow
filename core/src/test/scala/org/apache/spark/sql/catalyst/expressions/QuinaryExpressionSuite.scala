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

package org.apache.spark.sql.catalyst.expressions

import io.projectglow.sql.GlowBaseTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types.{DataType, IntegerType}

case class testQuinaryExpr(
    child1: Expression,
    child2: Expression,
    child3: Expression,
    child4: Expression,
    child5: Option[Expression])
    extends QuinaryExpression {

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = Seq(child1, child2, child3, child4) ++ child5

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      (c1, c2, c3, c4, c5) => {
        val c5String = c5.map(s => s", $s").getOrElse("")
        s"""
           |
           |${ev.value} = testFun($c1, $c2, $c3, $c4$c5String);
       """.stripMargin
      }
    )
  }
  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): testQuinaryExpr =
    copy(
      child1 = newChildren.head,
      child2 = newChildren(1),
      child3 = newChildren(2),
      child4 = newChildren(4),
      child5 = Option(newChildren(5)))
}

class QuinaryExpressionSuite extends GlowBaseTest {
  test("nullSafeCodeGen for un-nullable expression with Some argument") {
    val ctx = new CodegenContext
    val testExpr = testQuinaryExpr(
      Literal("c1"),
      Literal("c2"),
      Literal("c3"),
      Literal("c4"),
      Some(Literal("c5")))
    val myCode = testExpr.doGenCode(
      ctx,
      ExprCode(
        VariableValue("isNull_var", classOf[Boolean]),
        VariableValue("test_var", classOf[InternalRow])
      ))
    assert(
      myCode
        .code
        .toString == "int test_var = -1;\n        \n\ntest_var = testFun(((UTF8String) references[0] /* literal */), ((UTF8String) references[1] /* literal */), ((UTF8String) references[2] /* literal */), ((UTF8String) references[3] /* literal */), ((UTF8String) references[4] /* literal */));")
    assert(myCode.isNull == FalseLiteral)
  }

  test("nullSafeCodeGen for un-nullable expression with None argument") {
    val ctx = new CodegenContext
    val testExpr = testQuinaryExpr(Literal("c1"), Literal("c2"), Literal("c3"), Literal("c4"), None)
    val myCode = testExpr.doGenCode(
      ctx,
      ExprCode(
        VariableValue("isNull_var", classOf[Boolean]),
        VariableValue("test_var", classOf[InternalRow])
      ))
    assert(
      myCode
        .code
        .toString == "int test_var = -1;\n        \n\ntest_var = testFun(((UTF8String) references[0] /* literal */), ((UTF8String) references[1] /* literal */), ((UTF8String) references[2] /* literal */), ((UTF8String) references[3] /* literal */));")
    assert(myCode.isNull == FalseLiteral)
  }
}
