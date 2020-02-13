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

package io.projectglow

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Expression, LambdaFunction, Literal, UnresolvedNamedLambdaVariable}

import io.projectglow.sql.expressions.ExpressionHelper

// scalastyle:off
// format: off

/**
 * Functions provided by Glow. These functions can be used with Spark's DataFrame API.
 * @group complex_type_manipulation 
 * @group etl 
 * @group quality_control 
 * @group gwas_functions 
 */
object functions {
  private def withExpr(expr: Expression): Column = {
    new Column(ExpressionHelper.wrapAggregate(ExpressionHelper.rewrite(expr)))
  }

  private def createLambda(f: Column => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq("x"))
    val function = f(new Column(x)).expr
    LambdaFunction(function, Seq(x))
  }

  private def createLambda(f: (Column, Column) => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq("x"))
    val y = UnresolvedNamedLambdaVariable(Seq("y"))
    val function = f(new Column(x), new Column(y)).expr
    LambdaFunction(function, Seq(x, y))
  }

  /**
   * Add fields to a struct
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param struct The struct to which fields will be added
   * @param fields New fields
   */
  def add_struct_fields(struct: Column, fields: Column*): Column = withExpr {
    new io.projectglow.sql.expressions.AddStructFields(struct.expr, fields.map(_.expr))
  }

  /**
   * Compute the min, max, mean, stddev for an array of numerics
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param arr The array of numerics
   */
  def array_summary_stats(arr: Column): Column = withExpr {
    new io.projectglow.sql.expressions.ArrayStatsSummary(arr.expr)
  }

  /**
   * Convert an array of numerics into a spark.ml DenseVector
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param arr The array of numerics
   */
  def array_to_dense_vector(arr: Column): Column = withExpr {
    new io.projectglow.sql.expressions.ArrayToDenseVector(arr.expr)
  }

  /**
   * Convert an array of numerics into a spark.ml SparseVector
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param arr The array of numerics
   */
  def array_to_sparse_vector(arr: Column): Column = withExpr {
    new io.projectglow.sql.expressions.ArrayToSparseVector(arr.expr)
  }

  /**
   * Promote fields of a nested struct to top-level columns. Similar to using struct.* from SQL, but can be used in more contexts.
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param struct The struct to expand
   */
  def expand_struct(struct: Column): Column = withExpr {
    new io.projectglow.sql.expressions.ExpandStruct(struct.expr)
  }

  /**
   * Explode a spark.ml Matrix into arrays of rows
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param matrix The matrix to explode
   */
  def explode_matrix(matrix: Column): Column = withExpr {
    new io.projectglow.sql.expressions.ExplodeMatrix(matrix.expr)
  }

  /**
   * Select fields from a struct
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param struct Struct from which to select fields
   * @param fields Fields to take
   */
  def subset_struct(struct: Column, fields: String*): Column = withExpr {
    new io.projectglow.sql.expressions.SubsetStruct(struct.expr, fields.map(Literal(_)))
  }

  /**
   * Convert a spark.ml vector (sparse or dense) to an array of doubles
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param vector Vector to convert
   */
  def vector_to_array(vector: Column): Column = withExpr {
    new io.projectglow.sql.expressions.VectorToArray(vector.expr)
  }

  /**
   * Converts an array of probabilities to hard calls
   * @group etl
   * @since 0.3.0
   *
   * @param probabilities Probabilities
   * @param numAlts The number of alts
   * @param phased Whether the probabilities are phased or not
   * @param threshold The minimum probability to include
   */
  def hard_calls(probabilities: Column, numAlts: Column, phased: Column, threshold: Double): Column = withExpr {
    new io.projectglow.sql.expressions.HardCalls(probabilities.expr, numAlts.expr, phased.expr, Literal(threshold))
  }

  def hard_calls(probabilities: Column, numAlts: Column, phased: Column): Column = withExpr {
    new io.projectglow.sql.expressions.HardCalls(probabilities.expr, numAlts.expr, phased.expr)
  }


  /**
   * Do liftover like Picard
   * @group etl
   * @since 0.3.0
   *
   * @param contigName The current contigName
   * @param start The current start
   * @param end The current end
   * @param chainFile Location of the chain file on each node in the cluster
   * @param minMatchRatio Minimum fraction of bases that must remap to lift over successfully
   */
  def lift_over_coordinates(contigName: Column, start: Column, end: Column, chainFile: String, minMatchRatio: Double): Column = withExpr {
    new io.projectglow.sql.expressions.LiftOverCoordinatesExpr(contigName.expr, start.expr, end.expr, Literal(chainFile), Literal(minMatchRatio))
  }

  def lift_over_coordinates(contigName: Column, start: Column, end: Column, chainFile: String): Column = withExpr {
    new io.projectglow.sql.expressions.LiftOverCoordinatesExpr(contigName.expr, start.expr, end.expr, Literal(chainFile))
  }


  /**
   * Compute custom per-sample aggregates
   * @group quality_control
   * @since 0.3.0
   *
   * @param arr array of values.
   * @param initialValue the initial value
   * @param update update function
   * @param merge merge function
   * @param evaluate evaluate function
   */
  def aggregate_by_index(arr: Column, initialValue: Column, update: (Column, Column) => Column, merge: (Column, Column) => Column, evaluate: Column => Column): Column = withExpr {
    new io.projectglow.sql.expressions.UnwrappedAggregateByIndex(arr.expr, initialValue.expr, createLambda(update), createLambda(merge), createLambda(evaluate))
  }

  def aggregate_by_index(arr: Column, initialValue: Column, update: (Column, Column) => Column, merge: (Column, Column) => Column): Column = withExpr {
    new io.projectglow.sql.expressions.UnwrappedAggregateByIndex(arr.expr, initialValue.expr, createLambda(update), createLambda(merge))
  }


  /**
   * Compute call stats for an array of genotype structs
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes The array of genotype structs
   */
  def call_summary_stats(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.CallStats(genotypes.expr)
  }

  /**
   * Compute summary statistics for depth field from array of genotype structs
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes The array of genotype structs
   */
  def dp_summary_stats(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.DpSummaryStats(genotypes.expr)
  }

  /**
   * Compute statistics relating to the Hardy Weinberg equilibrium
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes The array of genotype structs
   */
  def hardy_weinberg(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.HardyWeinberg(genotypes.expr)
  }

  /**
   * Compute summary statistics about the genotype quality field for an array of genotype structs
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes The array of genotype structs
   */
  def gq_summary_stats(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.GqSummaryStats(genotypes.expr)
  }

  /**
   * Compute per-sample call stats
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes The array of genotype structs
   * @param refAllele The reference allele
   * @param alternateAlleles An array of alternate alleles
   */
  def sample_call_summary_stats(genotypes: Column, refAllele: Column, alternateAlleles: Column): Column = withExpr {
    new io.projectglow.sql.expressions.CallSummaryStats(genotypes.expr, refAllele.expr, alternateAlleles.expr)
  }

  /**
   * Compute per-sample summary statistics about the depth field in an array of genotype structs
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes The array of genotype structs
   */
  def sample_dp_summary_stats(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.SampleDpSummaryStatistics(genotypes.expr)
  }

  /**
   * Compute per-sample summary statistics about the genotype quality field in an array of genotype structs
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes The array of genotype structs
   */
  def sample_gq_summary_stats(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.SampleGqSummaryStatistics(genotypes.expr)
  }

  /**
   * A linear regression GWAS function
   * @group gwas_functions
   * @since 0.3.0
   *
   * @param genotypes An array of genotypes
   * @param phenotypes An array of phenotypes
   * @param covariates A Spark matrix of covariates
   */
  def linear_regression_gwas(genotypes: Column, phenotypes: Column, covariates: Column): Column = withExpr {
    new io.projectglow.sql.expressions.LinearRegressionExpr(genotypes.expr, phenotypes.expr, covariates.expr)
  }

  /**
   * A logistic regression function
   * @group gwas_functions
   * @since 0.3.0
   *
   * @param genotypes An array of genotypes
   * @param phenotypes An array of phenotype values
   * @param covariates a matrix of covariates
   * @param test Which logistic regression test to use. Can be 'LRT' or 'Firth'
   */
  def logistic_regression_gwas(genotypes: Column, phenotypes: Column, covariates: Column, test: String): Column = withExpr {
    new io.projectglow.sql.expressions.LogisticRegressionExpr(genotypes.expr, phenotypes.expr, covariates.expr, Literal(test))
  }

  /**
   * Get number of alt alleles for a genotype
   * @group gwas_functions
   * @since 0.3.0
   *
   * @param genotypes An array of genotype structs
   */
  def genotype_states(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.GenotypeStates(genotypes.expr)
  }
}