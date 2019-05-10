/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.hls.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct
import org.apache.spark.sql.databricks.hls.tertiary.ExpandStruct

import com.databricks.hls.tertiary._
import com.databricks.vcf.VariantSchemas

object SqlExtensionProvider {
  def registerFunctions(sess: SparkSession): Unit = {
    val functionRegistry = sess.sessionState.functionRegistry
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
  }
}
