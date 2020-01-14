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
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OrderPreservingUnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType
import io.projectglow.sql.expressions.{AddStructFields, AggregateByIndex, UnwrappedAggregateFunction}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode}

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

/**
 * Hacky catalyst structures to insert wholestage codegen boundaries. Can be useful if
 * you're hitting the JVM method limits in generated code.
 */
case class WholestageCodegenBoundaryPlan(child: LogicalPlan) extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class WholestageCodegenBoundaryStrategy() extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case WholestageCodegenBoundaryPlan(child) =>
      Seq(WholestageCodegenBoundaryExec(planLater(child)))
    case _ => Seq.empty[SparkPlan]
  }
}

case class WholestageCodegenBoundaryExec(child: SparkPlan)
    extends UnaryExecNode
    with CodegenSupport {

  override def output: Seq[Attribute] = child.output
  override def doExecute(): RDD[InternalRow] = child.execute()
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()
  override def usedInputs: AttributeSet = inputSet

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    parent.doConsume(ctx, input, row)
  }
}
