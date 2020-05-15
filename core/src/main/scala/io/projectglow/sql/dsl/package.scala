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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._

package object dsl {

  def makeLambdaFunction(f: Expression => Expression): LambdaFunction = {
    val x = UnresolvedNamedLambdaVariable(Seq("lv"))
    LambdaFunction(f(x), Seq(x))
  }

  def makeLambdaFunction(f: (Expression, Expression) => Expression): LambdaFunction = {
    val x = UnresolvedNamedLambdaVariable(Seq("lv1"))
    val y = UnresolvedNamedLambdaVariable(Seq("lv2"))
    LambdaFunction(f(x, y), Seq(x, y))
  }

  trait ImplicitOperators {
    def expr: Expression
    def arrayTransform(f: Expression => Expression): Expression = {
      ArrayTransform(expr, makeLambdaFunction(f))
    }
    def arrayTransform(f: (Expression, Expression) => Expression): Expression = {
      ArrayTransform(expr, makeLambdaFunction(f))
    }
    def filter(f: Expression => Expression): Expression = {
      ArrayFilter(expr, makeLambdaFunction(f))
    }
    def filter(f: (Expression, Expression) => Expression): Expression = {
      ArrayFilter(expr, makeLambdaFunction(f))
    }
    def aggregate(
        initialValue: Expression,
        merge: (Expression, Expression) => Expression,
        finish: Expression => Expression = identity): Expression = {
      ArrayAggregate(
        expr,
        initialValue,
        makeLambdaFunction(merge),
        makeLambdaFunction(finish)
      )
    }
  }

  implicit class GlowExpression(val expr: Expression) extends ImplicitOperators
}
