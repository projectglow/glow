package com.databricks.hls.sql.optimizer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OrderPreservingUnaryNode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode}

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
