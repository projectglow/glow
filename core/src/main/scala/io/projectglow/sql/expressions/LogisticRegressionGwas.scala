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

package io.projectglow.sql.expressions

import breeze.linalg._
import breeze.numerics._
import com.google.common.annotations.VisibleForTesting
import org.apache.commons.math3.distribution.{ChiSquaredDistribution, NormalDistribution}
import org.apache.spark.ml.linalg.{DenseMatrix => SparkDenseMatrix}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, CaseInsensitiveMap}
import org.apache.spark.sql.types.StructType

import io.projectglow.common.GlowLogging

/**
 * Statistics returned upon performing a logit test.
 *
 * @param beta Log-odds associated with the genotype, NaN if the null/full model fit failed
 * @param oddsRatio Odds ratio associated with the genotype, NaN if the null/full model fit failed
 * @param waldConfidenceInterval Wald 95% confidence interval of the odds ratio, NaN if the null/full model fit failed
 * @param pValue P-value for the specified test, NaN if the null/full model fit failed. Determined using the profile likelihood method.
 */
case class LogitTestResults(
    beta: Double,
    oddsRatio: Double,
    waldConfidenceInterval: Seq[Double],
    pValue: Double)

object LogitTestResults {
  val nanRow: InternalRow = InternalRow(NaN, NaN, ArrayData.toArrayData(Seq(NaN, NaN)), NaN)
}

/**
 * Some of the logic used for logistic regression is from the Hail project.
 * The Hail project can be found on Github: https://github.com/hail-is/hail.
 * The Hail project is under an MIT license: https://github.com/hail-is/hail/blob/master/LICENSE.
 */
object LogisticRegressionGwas extends GlowLogging {
  val logitTests: Map[String, LogitTest] = CaseInsensitiveMap(
    Map(
      "lrt" -> LikelihoodRatioTest,
      "firth" -> FirthTest
    ))
  val zScore: Double = new NormalDistribution().inverseCumulativeProbability(.975) // Two-sided 95% confidence

  @VisibleForTesting
  private[projectglow] def newtonIterations(
      X: DenseMatrix[Double],
      y: DenseVector[Double],
      hessianPlaceHolder: DenseMatrix[Double],
      args: NewtonIterationsState,
      maxIter: Int = 25,
      tolerance: Double = 1e-6): NewtonResult = {

    var iter = 1
    var converged = false
    var exploded = false

    val deltaB = DenseVector.zeros[Double](X.cols)

    while (!converged && !exploded && iter <= maxIter) {
      try {
        deltaB := args.fisher \ args.score // Solve for Newton-Raphson step

        if (deltaB(0).isNaN) {
          exploded = true
        } else if (max(abs(deltaB)) < tolerance) {
          converged = true
        } else {
          iter += 1
          args.b += deltaB // Parameter update
          args.mu := X * args.b // Fitted probability
          sigmoid.inPlace(args.mu)
          args.score := X.t * (y - args.mu) // Gradient
          hessianPlaceHolder := X
          hessianPlaceHolder(::, *) :*= (args.mu *:* (1d - args.mu))
          args.fisher := X.t * hessianPlaceHolder // Hessian
        }
      } catch {
        case _: breeze.linalg.MatrixSingularException => exploded = true
        case _: breeze.linalg.NotConvergedException => exploded = true
      }
    }

    val statistic = (y *:* args.mu) + ((1d - y) *:* (1d - args.mu))
    breeze.numerics.log.inPlace(statistic)
    val logLkhd = sum(statistic)

    NewtonResult(args, logLkhd, iter, converged, exploded)
  }

  /**
   * Generate an [[InternalRow]] with [[LogitTestResults]] schema based on the outputs of a
   * logit test.
   */
  private[projectglow] def makeStats(
      beta: Double,
      fisher: DenseMatrix[Double],
      fullFitLogLkhd: Double,
      nullFitLogLkhd: Double): InternalRow = {
    val oddsRatio = math.exp(beta)

    val covarianceMatrix = inv(fisher)
    val variance = diag(covarianceMatrix)
    val standardError = math.sqrt(variance(-1))
    val halfWidth = LogisticRegressionGwas.zScore * standardError
    val waldConfidenceInterval = Array(beta - halfWidth, beta + halfWidth).map(math.exp)

    val chi2 = 2 * (fullFitLogLkhd - nullFitLogLkhd)
    val df = 1
    val chi2Dist = new ChiSquaredDistribution(df)
    val pValue = 1 - chi2Dist.cumulativeProbability(Math.abs(chi2)) // 1-sided p-value

    InternalRow(beta, oddsRatio, ArrayData.toArrayData(waldConfidenceInterval), pValue)
  }
}

class NewtonIterationsState(numRows: Int, numCols: Int) {
  val b: DenseVector[Double] = DenseVector.zeros[Double](numCols)
  val mu: DenseVector[Double] = DenseVector.zeros[Double](numRows)
  val score: DenseVector[Double] = DenseVector.zeros[Double](numCols)
  val fisher: DenseMatrix[Double] = DenseMatrix.zeros[Double](numCols, numCols)

  def initFromMatrix(X: DenseMatrix[Double], y: DenseVector[Double]): Unit = {

    val avg = sum(y) / X.rows
    b(0) = math.log(avg / (1 - avg))
    mu := X * b
    sigmoid.inPlace(mu)
    score := X.t * (y - mu)
    fisher := X.t * (X(::, *) *:* (mu *:* (1d - mu)))
  }

  def initFromMatrixAndNullFit(
      X: DenseMatrix[Double],
      y: DenseVector[Double],
      nullFitArgs: NewtonIterationsState): Unit = {

    val m0 = nullFitArgs.b.length

    val r0 = 0 until m0
    val r1 = m0 to -1

    val X0 = X(::, r0)
    val X1 = X(::, r1)

    b(r0) := nullFitArgs.b
    mu := X * b
    sigmoid.inPlace(mu)
    score(r0) := nullFitArgs.score
    score(r1) := X1.t * (y - mu)
    fisher(r0, r0) := nullFitArgs.fisher
    fisher(r0, r1) := X0.t * (X1(::, *) *:* (mu *:* (1d - mu)))
    fisher(r1, r0) := fisher(r0, r1).t
    fisher(r1, r1) := X1.t * (X1(::, *) *:* (mu *:* (1d - mu)))
  }
}

case class NewtonResult(
    args: NewtonIterationsState,
    logLkhd: Double,
    nIter: Int,
    converged: Boolean,
    exploded: Boolean)

/** Base trait for logistic regression tests */
trait LogitTest extends Serializable {

  /**
   * Type for the state that the test maintains between variants.
   */
  type FitState

  def resultSchema: StructType

  /**
   * If true, the [[FitState]] uses per-phenotype information and must be refit for each phenotype.
   * If false, the [[FitState]] only depends on the `covariates`
   *
   * @return
   */
  def fitStatePerPhenotype: Boolean

  /**
   * Initializes a [[FitState]] for a (covariate matrix, phenotype array) pair.
   *
   * As much memory allocation as possible should be performed in this step to avoid allocations
   * in the per-row fit.
   */
  def init(phenotypes: Array[Double], covariates: SparkDenseMatrix): FitState

  def runTest(
      genotypes: DenseVector[Double],
      phenotypes: DenseVector[Double],
      fitState: FitState): InternalRow
}
