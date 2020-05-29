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
   * Adds fields to a struct.
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param struct The struct to which fields will be added
   * @param fields The new fields to add. The arguments must alternate between string-typed literal field names and field values.
   * @return A struct consisting of the input struct and the added fields
   */
  def add_struct_fields(struct: Column, fields: Column*): Column = withExpr {
    new io.projectglow.sql.expressions.AddStructFields(struct.expr, fields.map(_.expr))
  }

  /**
   * Computes the minimum, maximum, mean, standard deviation for an array of numerics.
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param arr An array of any numeric type
   * @return A struct containing double ``mean``, ``stdDev``, ``min``, and ``max`` fields
   */
  def array_summary_stats(arr: Column): Column = withExpr {
    new io.projectglow.sql.expressions.ArrayStatsSummary(arr.expr)
  }

  /**
   * Converts an array of numerics into a ``spark.ml`` ``DenseVector``.
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param arr The array of numerics
   * @return A ``spark.ml`` ``DenseVector``
   */
  def array_to_dense_vector(arr: Column): Column = withExpr {
    new io.projectglow.sql.expressions.ArrayToDenseVector(arr.expr)
  }

  /**
   * Converts an array of numerics into a ``spark.ml`` ``SparseVector``.
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param arr The array of numerics
   * @return A ``spark.ml`` ``SparseVector``
   */
  def array_to_sparse_vector(arr: Column): Column = withExpr {
    new io.projectglow.sql.expressions.ArrayToSparseVector(arr.expr)
  }

  /**
   * Promotes fields of a nested struct to top-level columns similar to using ``struct.*`` from SQL, but can be used in more contexts.
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param struct The struct to expand
   * @return Columns corresponding to fields of the input struct
   */
  def expand_struct(struct: Column): Column = withExpr {
    new io.projectglow.sql.expressions.ExpandStruct(struct.expr)
  }

  /**
   * Explodes a ``spark.ml`` ``Matrix`` (sparse or dense) into multiple arrays, one per row of the matrix.
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param matrix The ``sparl.ml`` ``Matrix`` to explode
   * @return An array column in which each row is a row of the input matrix
   */
  def explode_matrix(matrix: Column): Column = withExpr {
    new io.projectglow.sql.expressions.ExplodeMatrix(matrix.expr)
  }

  /**
   * Selects fields from a struct.
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param struct Struct from which to select fields
   * @param fields Fields to select
   * @return A struct containing only the indicated fields
   */
  def subset_struct(struct: Column, fields: String*): Column = withExpr {
    new io.projectglow.sql.expressions.SubsetStruct(struct.expr, fields.map(Literal(_)))
  }

  /**
   * Converts a ``spark.ml`` ``Vector`` (sparse or dense) to an array of doubles.
   * @group complex_type_manipulation
   * @since 0.3.0
   *
   * @param vector Vector to convert
   * @return An array of doubles
   */
  def vector_to_array(vector: Column): Column = withExpr {
    new io.projectglow.sql.expressions.VectorToArray(vector.expr)
  }

  /**
   * Converts an array of probabilities to hard calls. The probabilities are assumed to be diploid. See :ref:`variant-data-transformations` for more details.
   * @group etl
   * @since 0.3.0
   *
   * @param probabilities The array of probabilities to convert
   * @param numAlts The number of alternate alleles
   * @param phased Whether the probabilities are phased. If phased, we expect one ``2 * numAlts`` values in the probabilities array. If unphased, we expect one probability per possible genotype.
   * @param threshold The minimum probability to make a call. If no probability falls into the range of ``[0, 1 - threshold]`` or ``[threshold, 1]``, a no-call (represented by ``-1`` s) will be emitted. If not provided, this parameter defaults to ``0.9``.
   * @return An array of hard calls
   */
  def hard_calls(probabilities: Column, numAlts: Column, phased: Column, threshold: Double): Column = withExpr {
    new io.projectglow.sql.expressions.HardCalls(probabilities.expr, numAlts.expr, phased.expr, Literal(threshold))
  }

  def hard_calls(probabilities: Column, numAlts: Column, phased: Column): Column = withExpr {
    new io.projectglow.sql.expressions.HardCalls(probabilities.expr, numAlts.expr, phased.expr)
  }


  /**
   * Performs liftover for the coordinates of a variant. To perform liftover of alleles and add additional metadata, see :ref:`liftover`.
   * @group etl
   * @since 0.3.0
   *
   * @param contigName The current contig name
   * @param start The current start
   * @param end The current end
   * @param chainFile Location of the chain file on each node in the cluster
   * @param minMatchRatio Minimum fraction of bases that must remap to do liftover successfully. If not provided, defaults to ``0.95``.
   * @return A struct containing ``contigName``, ``start``, and ``end`` fields after liftover
   */
  def lift_over_coordinates(contigName: Column, start: Column, end: Column, chainFile: String, minMatchRatio: Double): Column = withExpr {
    new io.projectglow.sql.expressions.LiftOverCoordinatesExpr(contigName.expr, start.expr, end.expr, Literal(chainFile), Literal(minMatchRatio))
  }

  def lift_over_coordinates(contigName: Column, start: Column, end: Column, chainFile: String): Column = withExpr {
    new io.projectglow.sql.expressions.LiftOverCoordinatesExpr(contigName.expr, start.expr, end.expr, Literal(chainFile))
  }


  /**
   * Normalizes the variant with a behavior similar to vt normalize or bcftools norm.
   * Creates a StructType column including the normalized ``start``, ``end``, ``referenceAllele`` and
   * ``alternateAlleles`` fields (whether they are changed or unchanged as the result of
   * normalization) as well as a StructType field called ``normalizationStatus`` that
   * contains the following fields:
   * 
   *    ``changed``: A boolean field indicating whether the variant data was changed as a result of normalization
   * 
   *    ``errorMessage``: An error message in case the attempt at normalizing the row hit an error. In this case, the ``changed`` field will be set to ``false``. If no errors occur, this field will be ``null``.
   * 
   * In case of an error, the ``start``, ``end``, ``referenceAllele`` and ``alternateAlleles`` fields in the generated struct will be ``null``.
   * 
   * @group etl
   * @since 0.3.0
   *
   * @param contigName The current contig name
   * @param start The current start
   * @param end The current end
   * @param refAllele The current reference allele
   * @param altAlleles The current array of alternate alleles
   * @param refGenomePathString A path to the reference genome ``.fasta`` file. The ``.fasta`` file must be accompanied with a ``.fai`` index file in the same folder.
   * @return A struct as explained above
   */
  def normalize_variant(contigName: Column, start: Column, end: Column, refAllele: Column, altAlleles: Column, refGenomePathString: String): Column = withExpr {
    new io.projectglow.sql.expressions.NormalizeVariantExpr(contigName.expr, start.expr, end.expr, refAllele.expr, altAlleles.expr, Literal(refGenomePathString))
  }

  /**
   * Substitutes the missing values of a numeric array using the mean of the non-missing values. Any values that are NaN, null or equal to the missing value parameter are considered missing. See :ref:`variant-data-transformations` for more details.
   * @group etl
   * @since 0.4.0
   *
   * @param array A numeric array that may contain missing values
   * @param missingValue A value that should be considered missing. If not provided, this parameter defaults to ``-1``.
   * @return A numeric array with substituted missing values
   */
  def mean_substitute(array: Column, missingValue: Column): Column = withExpr {
    new io.projectglow.sql.expressions.MeanSubstitute(array.expr, missingValue.expr)
  }

  def mean_substitute(array: Column): Column = withExpr {
    new io.projectglow.sql.expressions.MeanSubstitute(array.expr)
  }


  /**
   * Computes custom per-sample aggregates.
   * @group quality_control
   * @since 0.3.0
   *
   * @param arr array of values.
   * @param initialValue the initial value
   * @param update update function
   * @param merge merge function
   * @param evaluate evaluate function
   * @return An array of aggregated values. The number of elements in the array is equal to the number of samples.
   */
  def aggregate_by_index(arr: Column, initialValue: Column, update: (Column, Column) => Column, merge: (Column, Column) => Column, evaluate: Column => Column): Column = withExpr {
    new io.projectglow.sql.expressions.UnwrappedAggregateByIndex(arr.expr, initialValue.expr, createLambda(update), createLambda(merge), createLambda(evaluate))
  }

  def aggregate_by_index(arr: Column, initialValue: Column, update: (Column, Column) => Column, merge: (Column, Column) => Column): Column = withExpr {
    new io.projectglow.sql.expressions.UnwrappedAggregateByIndex(arr.expr, initialValue.expr, createLambda(update), createLambda(merge))
  }


  /**
   * Computes call summary statistics for an array of genotype structs. See :ref:`variant-qc` for more details.
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes The array of genotype structs with ``calls`` field
   * @return A struct containing ``callRate``, ``nCalled``, ``nUncalled``, ``nHet``, ``nHomozygous``, ``nNonRef``, ``nAllelesCalled``, ``alleleCounts``, ``alleleFrequencies`` fields. See :ref:`variant-qc`.
   */
  def call_summary_stats(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.CallStats(genotypes.expr)
  }

  /**
   * Computes summary statistics for the depth field from an array of genotype structs. See :ref:`variant-qc`.
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes An array of genotype structs with ``depth`` field
   * @return A struct containing ``mean``, ``stdDev``, ``min``, and ``max`` of genotype depths
   */
  def dp_summary_stats(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.DpSummaryStats(genotypes.expr)
  }

  /**
   * Computes statistics relating to the Hardy Weinberg equilibrium. See :ref:`variant-qc` for more details.
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes The array of genotype structs with ``calls`` field
   * @return A struct containing two fields, ``hetFreqHwe`` (the expected heterozygous frequency according to Hardy-Weinberg equilibrium) and ``pValueHwe`` (the associated p-value)
   */
  def hardy_weinberg(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.HardyWeinberg(genotypes.expr)
  }

  /**
   * Computes summary statistics about the genotype quality field for an array of genotype structs. See :ref:`variant-qc`.
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes The array of genotype structs with ``conditionalQuality`` field
   * @return A struct containing ``mean``, ``stdDev``, ``min``, and ``max`` of genotype qualities
   */
  def gq_summary_stats(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.GqSummaryStats(genotypes.expr)
  }

  /**
   * Computes per-sample call summary statistics. See :ref:`sample-qc` for more details.
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes An array of genotype structs with ``calls`` field
   * @param refAllele The reference allele
   * @param alternateAlleles An array of alternate alleles
   * @return A struct containing ``sampleId``, ``callRate``, ``nCalled``, ``nUncalled``, ``nHomRef``, ``nHet``, ``nHomVar``, ``nSnp``, ``nInsertion``, ``nDeletion``, ``nTransition``, ``nTransversion``, ``nSpanningDeletion``, ``rTiTv``, ``rInsertionDeletion``, ``rHetHomVar`` fields. See :ref:`sample-qc`.
   */
  def sample_call_summary_stats(genotypes: Column, refAllele: Column, alternateAlleles: Column): Column = withExpr {
    new io.projectglow.sql.expressions.CallSummaryStats(genotypes.expr, refAllele.expr, alternateAlleles.expr)
  }

  /**
   * Computes per-sample summary statistics about the depth field in an array of genotype structs.
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes An array of genotype structs with ``depth`` field
   * @return An array of structs where each struct contains ``mean``, ``stDev``, ``min``, and ``max`` of the genotype depths for a sample. If ``sampleId`` is present in a genotype, it will be propagated to the resulting struct as an extra field.
   */
  def sample_dp_summary_stats(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.SampleDpSummaryStatistics(genotypes.expr)
  }

  /**
   * Computes per-sample summary statistics about the genotype quality field in an array of genotype structs.
   * @group quality_control
   * @since 0.3.0
   *
   * @param genotypes An array of genotype structs with ``conditionalQuality`` field
   * @return An array of structs where each struct contains ``mean``, ``stDev``, ``min``, and ``max`` of the genotype qualities for a sample. If ``sampleId`` is present in a genotype, it will be propagated to the resulting struct as an extra field.
   */
  def sample_gq_summary_stats(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.SampleGqSummaryStatistics(genotypes.expr)
  }

  /**
   * Performs a linear regression association test optimized for performance in a GWAS setting. See :ref:`linear-regression` for details.
   * @group gwas_functions
   * @since 0.3.0
   *
   * @param genotypes A numeric array of genotypes
   * @param phenotypes A numeric array of phenotypes
   * @param covariates A ``spark.ml`` ``Matrix`` of covariates
   * @return A struct containing ``beta``, ``standardError``, and ``pValue`` fields. See :ref:`linear-regression`.
   */
  def linear_regression_gwas(genotypes: Column, phenotypes: Column, covariates: Column): Column = withExpr {
    new io.projectglow.sql.expressions.LinearRegressionExpr(genotypes.expr, phenotypes.expr, covariates.expr)
  }

  /**
   * Performs a logistic regression association test optimized for performance in a GWAS setting. See :ref:`logistic-regression` for more details.
   * @group gwas_functions
   * @since 0.3.0
   *
   * @param genotypes An numeric array of genotypes
   * @param phenotypes A double array of phenotype values
   * @param covariates A ``spark.ml`` ``Matrix`` of covariates
   * @param test Which logistic regression test to use. Can be ``LRT`` or ``Firth``
   * @return A struct containing ``beta``, ``oddsRatio``, ``waldConfidenceInterval``, and ``pValue`` fields. See :ref:`logistic-regression`.
   */
  def logistic_regression_gwas(genotypes: Column, phenotypes: Column, covariates: Column, test: String): Column = withExpr {
    new io.projectglow.sql.expressions.LogisticRegressionExpr(genotypes.expr, phenotypes.expr, covariates.expr, Literal(test))
  }

  /**
   * Gets the number of alternate alleles for an array of genotype structs. Returns ``-1`` if there are any ``-1`` s (no-calls) in the calls array.
   * @group gwas_functions
   * @since 0.3.0
   *
   * @param genotypes An array of genotype structs with ``calls`` field
   * @return An array of integers containing the number of alternate alleles in each call array
   */
  def genotype_states(genotypes: Column): Column = withExpr {
    new io.projectglow.sql.expressions.GenotypeStates(genotypes.expr)
  }
}