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
import org.apache.spark.sql.types.DataType

package object dsl {

  trait ImplicitOperators {
    def expr: Expression
    private def makeLambdaFunction(
        f: NamedLambdaVariable => Expression,
        dt: DataType): LambdaFunction = {
      val x = NamedLambdaVariable("lv", dt, true)
      LambdaFunction(f(x), Seq(x))
    }
    private def makeLambdaFunction(
        f: (NamedLambdaVariable, NamedLambdaVariable) => Expression,
        dt: (DataType, DataType)): LambdaFunction = {
      val x = NamedLambdaVariable("lv1", dt._1, true)
      val y = NamedLambdaVariable("lv2", dt._2, true)
      LambdaFunction(f(x, y), Seq(x, y))
    }
    def arrayTransform(f: NamedLambdaVariable => Expression, dt: DataType): Expression = {
      ArrayTransform(expr, makeLambdaFunction(f, dt))
    }
    def arrayTransform(
        f: (NamedLambdaVariable, NamedLambdaVariable) => Expression,
        dt: (DataType, DataType)): Expression = {
      ArrayTransform(expr, makeLambdaFunction(f, dt))
    }
    def filter(f: NamedLambdaVariable => Expression, dt: DataType): Expression = {
      ArrayFilter(expr, makeLambdaFunction(f, dt))
    }
    def filter(
        f: (NamedLambdaVariable, NamedLambdaVariable) => Expression,
        dt: (DataType, DataType)): Expression = {
      ArrayFilter(expr, makeLambdaFunction(f, dt))
    }
    def aggregate(
        initialValue: Expression,
        mergeFunction: (NamedLambdaVariable, NamedLambdaVariable) => Expression,
        mergeDataTypes: (DataType, DataType),
        finishFunction: NamedLambdaVariable => Expression,
        finishDataType: DataType): Expression = {
      ArrayAggregate(
        expr,
        initialValue,
        makeLambdaFunction(mergeFunction, mergeDataTypes),
        makeLambdaFunction(finishFunction, finishDataType)
      )
    }
  }

  implicit class GlowExpression(val expr: Expression) extends ImplicitOperators
}
