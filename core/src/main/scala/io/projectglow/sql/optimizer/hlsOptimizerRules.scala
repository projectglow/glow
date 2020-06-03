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

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

import io.projectglow.common.GlowLogging
import io.projectglow.sql.expressions._
import io.projectglow.sql.util.{ExpectsGenotypeFields, RewriteAfterResolution}

/**
 * Simple optimization rule that handles expression rewrites
 */
object ReplaceExpressionsRule extends Rule[LogicalPlan] with GlowLogging {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressions {
      case expr: RewriteAfterResolution =>
        ExpressionHelper.wrapAggregate(expr.rewrite)
      case expr =>
        expr
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

/**
 * Handles [[ExpandStruct]] commands similarly to how [[org.apache.spark.sql.catalyst.analysis.Star]]
 * is handled in Spark. If the struct to expand has not yet been resolved, we intentionally
 * do nothing with the expectation that we have not yet reached a fixed point in analysis and
 * will be able to perform the expansion in a future iteration.
 */
object ResolveExpandStructRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUp {
      case p @ Project(projectList, _) if canExpand(projectList) =>
        p.copy(projectList = expandExprs(p.projectList))
      case a: Aggregate if canExpand(a.aggregateExpressions) =>
        a.copy(aggregateExpressions = expandExprs(a.aggregateExpressions))
    }
  }

  private def canExpand(projectList: Seq[Expression]): Boolean = projectList.exists {
    case e: ExpandStruct => e.childrenResolved
    case UnresolvedAlias(e: ExpandStruct, _) => e.childrenResolved
    case Alias(e: ExpandStruct, _) => e.childrenResolved
    case _ => false
  }

  private def expandExprs(exprs: Seq[NamedExpression]): Seq[NamedExpression] = {
    exprs.flatMap {
      case UnresolvedAlias(e: ExpandStruct, _) => e.expand()
      case Alias(e: ExpandStruct, _) => e.expand()
      case e: ExpandStruct => e.expand()
      case e => Seq(e)
    }
  }
}

/**
 * Resolve required genotype fields to their indices within the child expression. Performing
 * this resolution explicitly guards against expressions like [[org.apache.spark.sql.catalyst.expressions.ArraysZip]]
 * that can lose field names during physical planning.
 */
object ResolveGenotypeFields extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
    case e: ExpectsGenotypeFields
        if !e.resolved && e.childrenResolved && e
          .checkInputDataTypes() == TypeCheckResult.TypeCheckSuccess =>
      e.resolveGenotypeInfo()
  }
}
