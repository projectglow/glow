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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, LambdaFunction}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import io.projectglow.common.VariantSchemas
import io.projectglow.sql.expressions._
import io.projectglow.sql.optimizer.{HLSReplaceExpressionsRule, ResolveAggregateFunctionsRule}

// TODO(hhd): Spark 3.0 allows extensions to register functions. After Spark 3.0 is released,
// we should move all extensions into this class.
class GlowSQLExtensions extends (SparkSessionExtensions => Unit) {
  val resolutionRules: Seq[Rule[LogicalPlan]] = Seq(HLSReplaceExpressionsRule, ResolveAggregateFunctionsRule)
  val optimizations: Seq[Rule[LogicalPlan]] = Seq()
  def apply(extensions: SparkSessionExtensions): Unit = {

    resolutionRules.foreach(r => extensions.injectResolutionRule(_ => r))
    optimizations.foreach(r => extensions.injectOptimizerRule(_ => r))
  }
}

object SqlExtensionProvider {

  def registerFunctions(conf: SQLConf, functionRegistry: FunctionRegistry): Unit = {
    functionRegistry.registerFunction(
      FunctionIdentifier("add_struct_fields"),
      exprs => AddStructFields(exprs.head, exprs.tail)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("aggregate_by_index"),
      exprs =>
        UnwrappedAggregateByIndex(
          exprs(0),
          exprs(1),
          exprs(2),
          exprs(3),
          exprs.lift(4).getOrElse(LambdaFunction.identity))
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("array_summary_stats"),
      exprs => ArrayStatsSummary(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("array_to_dense_vector"),
      exprs => ArrayToDenseVector(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("array_to_sparse_vector"),
      exprs => ArrayToSparseVector(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("call_summary_stats"),
      exprs => CallStats(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("dp_summary_stats"),
      exprs => ArrayStatsSummary.makeDpStats(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("expand_struct"),
      exprs => ExpandStruct(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("explode_matrix"),
      exprs => ExplodeMatrix(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("genotype_states"),
      exprs => GenotypeStates(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("gq_summary_stats"),
      exprs => ArrayStatsSummary.makeGqStats(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("hard_calls"),
      exprs => HardCalls(exprs(0), exprs(1), exprs(2), exprs.lift(3))
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("hardy_weinberg"),
      exprs => HardyWeinberg(exprs.head)
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("lift_over_coordinates"),
      exprs => LiftOverCoordinatesExpr(exprs(0), exprs(1), exprs(2), exprs(3), exprs.lift(4))
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("linear_regression_gwas"),
      exprs => LinearRegressionExpr(exprs(0), exprs(1), exprs(2))
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("logistic_regression_gwas"),
      exprs => LogisticRegressionExpr(exprs(0), exprs(1), exprs(2), exprs(3))
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
      FunctionIdentifier("subset_struct"),
      exprs => {
        val struct = exprs.head
        val fields = exprs.tail
        CreateNamedStruct(fields.flatMap(f => Seq(f, UnresolvedExtractValue(struct, f))))
      }
    )

    functionRegistry.registerFunction(
      FunctionIdentifier("vector_to_array"),
      exprs => VectorToArray(exprs.head)
    )
  }
}
