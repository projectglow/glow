/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.hls.sql

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.databricks.hls.tertiary.{AddStructFields, ExpandStruct, ExplodeMatrix}
import org.apache.spark.sql.internal.SQLConf

import com.databricks.hls.sql.optimizer.HLSReplaceExpressionsRule
import com.databricks.hls.tertiary._
import com.databricks.vcf.VariantSchemas

object SqlExtensionProvider {

  final def register(session: SparkSession): Unit = {
    val experimental = session.experimental
    experimental.extraOptimizations ++= getExtraOptimizations
    experimental.extraStrategies ++= getExtraStrategies
    registerFunctions(session.sessionState.conf, session.sessionState.functionRegistry)
  }
  def getExtraOptimizations: Seq[Rule[LogicalPlan]] = {
    Seq(HLSReplaceExpressionsRule)
  }

  def getExtraStrategies: Seq[Strategy] = Nil

  def registerFunctions(conf: SQLConf, functionRegistry: FunctionRegistry): Unit = {
    functionRegistry.registerFunction(
      FunctionIdentifier("subset_struct"),
      exprs => {
        val struct = exprs.head
        val fields = exprs.tail
        CreateNamedStruct(fields.flatMap(f => Seq(f, UnresolvedExtractValue(struct, f))))
      }
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("hardy_weinberg"),
      exprs => HardyWeinberg(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("call_summary_stats"),
      exprs => CallStats(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("array_summary_stats"),
      exprs => ArrayStatsSummary(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("dp_summary_stats"),
      exprs => ArrayStatsSummary.makeDpStats(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("gq_summary_stats"),
      exprs => ArrayStatsSummary.makeGqStats(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("expand_struct"),
      exprs => ExpandStruct(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("sample_call_summary_stats"),
      exprs => CallSummaryStats(exprs(0), exprs(1), exprs(2))
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("sample_dp_summary_stats"),
      exprs => PerSampleSummaryStatistics(exprs.head, VariantSchemas.depthField)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("sample_gq_summary_stats"),
      exprs => PerSampleSummaryStatistics(exprs.head, VariantSchemas.conditionalQualityField)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("genotype_states"),
      exprs => GenotypeStates(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("add_struct_fields"),
      exprs => AddStructFields(exprs.head, exprs.tail)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("sample_call_summary_stats"),
      exprs => CallSummaryStats(exprs(0), exprs(1), exprs(2))
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("sample_dp_summary_stats"),
      exprs => PerSampleSummaryStatistics(exprs.head, VariantSchemas.depthField)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("sample_gq_summary_stats"),
      exprs => PerSampleSummaryStatistics(exprs.head, VariantSchemas.conditionalQualityField)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("genotype_states"),
      exprs => GenotypeStates(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("hard_calls"),
      exprs => HardCalls(exprs(0), exprs(1), exprs(2), exprs.lift(3))
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("array_to_sparse_vector"),
      exprs => ArrayToSparseVector(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("array_to_dense_vector"),
      exprs => ArrayToDenseVector(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("vector_to_array"),
      exprs => VectorToArray(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("linear_regression_gwas"),
      exprs => LinearRegressionExpr(exprs(0), exprs(1), exprs(2))
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("explode_matrix"),
      exprs => ExplodeMatrix(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("lift_over"),
      exprs => LiftOverExpr(exprs(0), exprs(1), exprs(2), exprs(3), exprs.lift(4))
    )
  }
}
