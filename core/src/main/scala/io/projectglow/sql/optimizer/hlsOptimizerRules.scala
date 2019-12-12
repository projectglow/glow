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

import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, GetArrayStructFields, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{ArrayType, StructType}

import io.projectglow.common.VariantSchemas
import io.projectglow.sql.expressions._

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
    case GenotypeStatesBase(genotypes) =>
      val gArray = genotypes.dataType.asInstanceOf[ArrayType]
      val gSchema = gArray.elementType.asInstanceOf[StructType]

      val idx = gSchema
        .indexWhere(SQLUtils.structFieldsEqualExceptNullability(_, VariantSchemas.callsField))
      val arr = GetArrayStructFields(genotypes, VariantSchemas.callsField, idx, gSchema.length, gArray.containsNull)
      GenotypeStates(arr)

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
