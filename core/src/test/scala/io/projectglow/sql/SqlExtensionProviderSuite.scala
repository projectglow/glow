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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Literal, UnaryExpression}
import org.apache.spark.sql.types.{DataType, IntegerType}

import io.projectglow.GlowSuite

class SqlExtensionProviderSuite extends GlowSuite {
  override def beforeEach(): Unit = {
    super.beforeEach()
    SqlExtensionProvider.registerFunctions(
      spark.sessionState.conf,
      spark.sessionState.functionRegistry,
      "test-functions.yml")
  }

  private lazy val sess = spark
  test("one arg function") {
    import sess.implicits._
    assert(spark.range(1).selectExpr("one_arg_test(id)").as[Int].head() == 1)

    intercept[AnalysisException] {
      spark.range(1).selectExpr("one_arg_test()").collect()
    }

    intercept[AnalysisException] {
      spark.range(1).selectExpr("one_arg_test(id, id)").collect()
    }
  }

  test("two arg function") {
    import sess.implicits._
    assert(spark.range(1).selectExpr("two_arg_test(id, id)").as[Int].head() == 1)

    intercept[AnalysisException] {
      spark.range(1).selectExpr("two_arg_test(id)").collect()
    }

    intercept[AnalysisException] {
      spark.range(1).selectExpr("two_arg_test(id, id, id)").collect()
    }
  }

  test("var args function") {
    import sess.implicits._
    assert(spark.range(1).selectExpr("var_args_test(id, id)").as[Int].head() == 1)
    assert(spark.range(1).selectExpr("var_args_test(id, id, id, id)").as[Int].head() == 1)
    assert(spark.range(1).selectExpr("var_args_test(id)").as[Int].head() == 1)

    intercept[AnalysisException] {
      spark.range(1).selectExpr("var_args_test()").collect()
    }
  }

  test("can call optional arg function") {
    import sess.implicits._
    assert(spark.range(1).selectExpr("optional_arg_test(id)").as[Int].head() == 1)
    assert(spark.range(1).selectExpr("optional_arg_test(id, id)").as[Int].head() == 1)

    intercept[AnalysisException] {
      spark.range(1).selectExpr("optional_arg_test()").collect()
    }

    intercept[AnalysisException] {
      spark.range(1).selectExpr("optional_arg_test(id, id, id)").collect()
    }
  }
}

trait TestExpr extends Expression with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = 1
}

case class OneArgExpr(child: Expression) extends UnaryExpression with TestExpr {
  override def withNewChildInternal(newChild: Expression): OneArgExpr =
    copy(child = newChild)
}
case class TwoArgExpr(left: Expression, right: Expression) extends BinaryExpression with TestExpr {
  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): TwoArgExpr =
    copy(left = newLeft, right = newRight)
}
case class VarArgsExpr(arg: Expression, varArgs: Seq[Expression]) extends TestExpr {
  override def children: Seq[Expression] = arg +: varArgs
  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): VarArgsExpr =
    copy(arg = newChildren.head, varArgs = newChildren.drop(1))
}
case class OptionalArgExpr(required: Expression, optional: Expression) extends TestExpr {
  def this(required: Expression) = this(required, Literal(1))
  override def children: Seq[Expression] = Seq(required, optional)
  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): OptionalArgExpr =
    copy(required = newChildren.head, optional = newChildren.last)
}
