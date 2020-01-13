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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

import io.projectglow.sql.expressions._
import io.projectglow.sql.util.RewriteAfterResolution

/**
 * Simple optimization rule that handles expression rewrites
 */
object ReplaceExpressionsRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressions {
      case expr: RewriteAfterResolution =>
        ExpressionHelper.wrapAggregate(expr.rewrite)
    }
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
      ExpressionHelper.wrapAggregate(agg.asWrapped)
  }
}
