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

package io.projectglow.sql.optimizer

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete}
import org.apache.spark.sql.catalyst.expressions.{ArrayMax, ArrayMin, ArrayTransform, CaseWhen, CreateNamedStruct, ElementAt, GetStructField, GreaterThan, LambdaFunction, LessThan, Literal, NamedLambdaVariable, Size, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType
import io.projectglow.sql.expressions.{AddStructFields, AggregateByIndex, PermuteArray, UnwrappedAggregateFunction}
import javassist.bytecode.stackmap.TypeData.ArrayElement

/**
 * Simple optimization rule that handles expression rewrites
 */
object HLSReplaceExpressionsRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
    case AddStructFields(struct, newFields) =>
      val baseType = struct.dataType.asInstanceOf[StructType]
      val baseFields = baseType.indices.flatMap { idx =>
        Seq(Literal(baseType(idx).name), GetStructField(struct, idx))
      }
      CreateNamedStruct(baseFields ++ newFields)
    case PermuteArray(dataArray, indexArray) =>
      val maxIndexCheck = GreaterThan(ArrayMax(indexArray), Size(dataArray))
      val minIndexCheck = LessThan(ArrayMin(indexArray), Literal(1))
      val elseValue = {
        val elementArg = UnresolvedNamedLambdaVariable(Seq("el"))
        val fn = ElementAt(dataArray, elementArg)
        ArrayTransform(indexArray, LambdaFunction(fn, Seq(elementArg)))
      }
      val branches = Seq(
        (maxIndexCheck, Literal(null)),
        (minIndexCheck, Literal(null))
      )
      CaseWhen(branches, elseValue = elseValue)
  }
}

/**
 * This rule is needed by [[AggregateByIndex]].
 *
 * Spark's analyzer only wraps AggregateFunctions in AggregateExpressions immediately after
 * resolution. Since [[AggregateByIndex]] is first resolved as a higher order function, it is
 * not correctly wrapped. Note that it's merely a coincidence that it is first resolved as a higher
 * order function.
 */
object ResolveAggregateFunctionsRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformExpressions {
    case agg: UnwrappedAggregateFunction =>
      AggregateExpression(agg.asWrapped, Complete, isDistinct = false)
  }
}
