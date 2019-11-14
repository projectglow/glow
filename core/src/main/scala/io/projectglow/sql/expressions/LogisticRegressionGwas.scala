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
import com.github.fommil.netlib.LAPACK
import com.google.common.annotations.VisibleForTesting
import org.apache.commons.math3.distribution.{ChiSquaredDistribution, NormalDistribution}
import org.apache.spark.ml.linalg.{DenseMatrix => SparkDenseMatrix}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, CaseInsensitiveMap}
import org.apache.spark.sql.types.StructType
import org.netlib.util.intW

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
      nullFitOpt: Option[NewtonIterationsState],
      maxIter: Int = 25,
      tolerance: Double = 1e-6): NewtonResult = {

    val args = new NewtonIterationsState(X, y, nullFitOpt)

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
          args.mu := sigmoid(X * args.b) // Fitted probability
          args.score := X.t * (y - args.mu) // Gradient
          args.fisher := X.t * (X(::, *) *:* (args.mu *:* (1d - args.mu))) // Hessian
        }
      } catch {
        case _: breeze.linalg.MatrixSingularException => exploded = true
        case _: breeze.linalg.NotConvergedException => exploded = true
      }
    }

    val logLkhd = sum(breeze.numerics.log((y *:* args.mu) + ((1d - y) *:* (1d - args.mu))))

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

class NewtonIterationsState(
    X: DenseMatrix[Double],
    y: DenseVector[Double],
    nullFitArgsOpt: Option[NewtonIterationsState]) {

  val n = X.rows
  val m = X.cols
  val b: DenseVector[Double] = DenseVector.zeros[Double](m)
  val mu: DenseVector[Double] = DenseVector.zeros[Double](n)
  val score: DenseVector[Double] = DenseVector.zeros[Double](m)
  val fisher: DenseMatrix[Double] = DenseMatrix.zeros[Double](m, m)

  if (nullFitArgsOpt.isEmpty) {
    require(X.cols > 0, "Covariate matrix must have at least one column")

    val avg = sum(y) / n
    b(0) = math.log(avg / (1 - avg))
    mu := sigmoid(X * b)
    score := X.t * (y - mu)
    fisher := X.t * (X(::, *) *:* (mu *:* (1d - mu)))
  } else {
    // Warm-start from null model fit
    val nullFitArgs = nullFitArgsOpt.get
    val m0 = nullFitArgs.b.length

    val r0 = 0 until m0
    val r1 = m0 to -1

    val X0 = X(::, r0)
    val X1 = X(::, r1)

    b(r0) := nullFitArgs.b
    mu := sigmoid(X * b)
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

trait LogitTest extends Serializable {
  type FitState
  def resultSchema: StructType
  def canReuseNullFit: Boolean
  def fitNullModel(phenotypes: Array[Double], covariates: SparkDenseMatrix): FitState
  def runTest(
      x: DenseMatrix[Double],
      y: DenseVector[Double],
      nullModelFit: Option[FitState]): InternalRow
}

trait LogitTestWithoutNullFit extends LogitTest {
  final override val canReuseNullFit: Boolean = false

  final override def fitNullModel(
      phenotypes: Array[Double],
      covariates: SparkDenseMatrix): FitState = {
    throw new IllegalArgumentException("Test does not support null model fit")
  }
}

trait LogitTestWithNullFit extends LogitTest {
  final override val canReuseNullFit: Boolean = true
}

object LikelihoodRatioTest extends LogitTestWithNullFit {
  override type FitState = NewtonResult
  override def resultSchema: StructType = Encoders.product[LogitTestResults].schema

  override def fitNullModel(
      phenotypes: Array[Double],
      covariates: SparkDenseMatrix): NewtonResult = {
    val nullX = new DenseMatrix(covariates.numRows, covariates.numCols, covariates.values)
    val y = new DenseVector(phenotypes)
    LogisticRegressionGwas.newtonIterations(nullX, y, None)
  }

  override def runTest(
      x: DenseMatrix[Double],
      y: DenseVector[Double],
      nullFitOpt: Option[NewtonResult]): InternalRow = {
    val nullFit = nullFitOpt.getOrElse(throw new IllegalArgumentException("Null fit required"))
    val fullFit = LogisticRegressionGwas.newtonIterations(x, y, Some(nullFit.args))

    if (!nullFit.converged || !fullFit.converged) {
      return InternalRow(NaN, NaN, ArrayData.toArrayData(Seq(NaN, NaN)), NaN)
    }

    val beta = fullFit.args.b(-1)
    LogisticRegressionGwas.makeStats(beta, fullFit.args.fisher, fullFit.logLkhd, nullFit.logLkhd)
  }
}

case class FirthFitState(
    b: DenseVector[Double],
    logLkhd: Double,
    fisher: DenseMatrix[Double],
    converged: Boolean,
    exploded: Boolean)

object FirthTest extends LogitTestWithoutNullFit {
  override type FitState = FirthFitState
  override def resultSchema: StructType = Encoders.product[LogitTestResults].schema

  override def runTest(
      x: DenseMatrix[Double],
      y: DenseVector[Double],
      nullModelFit: Option[FirthFitState]): InternalRow = {
    val m = x.cols
    val m0 = m - 1

    val b0 = DenseVector.zeros[Double](m0)
    val nullFit = fitFirth(x, y, b0)
    val b = DenseVector.zeros[Double](m)
    b(0 until m0) := nullFit.b
    val fullFit = fitFirth(x, y, b)

    if (!nullFit.converged || !fullFit.converged) {
      return InternalRow(NaN, NaN, ArrayData.toArrayData(Seq(NaN, NaN)), NaN)
    }

    val beta = fullFit.b(-1)
    LogisticRegressionGwas.makeStats(beta, fullFit.fisher, fullFit.logLkhd, nullFit.logLkhd)
  }

  // Adapted from Hail's `fitFirth` method.
  def fitFirth(
      x: DenseMatrix[Double],
      y: DenseVector[Double],
      b0: DenseVector[Double],
      maxIter: Int = 100,
      tol: Double = 1e-6): FirthFitState = {

    require(b0.length <= x.cols)

    val b = b0.copy
    val m0 = b0.length
    var logLkhd = 0d
    var iter = 1
    var converged = false
    var exploded = false
    val mu: DenseVector[Double] = DenseVector.zeros[Double](x.rows)
    var sqrtW: DenseVector[Double] = null

    while (!converged && !exploded && iter <= maxIter) {
      try {
        mu := sigmoid(x(::, 0 until m0) * b)
        sqrtW = sqrt(mu *:* (1d - mu))
        val QR = qr.reduced(x(::, *) *:* sqrtW)
        val h = QR.q(*, ::).map(r => r dot r)
        val deltaB = solveUpperTriangular(
          QR.r(0 until m0, 0 until m0),
          QR.q(::, 0 until m0).t * (((y - mu) + (h *:* (0.5 - mu))) /:/ sqrtW))

        if (deltaB(0).isNaN) {
          exploded = true
        } else if (max(abs(deltaB)) < tol && iter > 1) {
          converged = true
          logLkhd = sum(breeze.numerics.log((y *:* mu) + ((1d - y) *:* (1d - mu)))) + sum(
              log(abs(diag(QR.r))))
        } else {
          iter += 1
          b += deltaB
        }
      } catch {
        case e: breeze.linalg.MatrixSingularException => exploded = true
        case e: breeze.linalg.NotConvergedException => exploded = true
      }
    }

    val fisher = x.t * (x(::, *) *:* (mu *:* (1d - mu)))
    FirthFitState(b, logLkhd, fisher, converged, exploded)
  }

  // Solve an upper triangular system of equations using LAPACK.
  // Adapted from Hail's TriSolver
  def solveUpperTriangular(a: DenseMatrix[Double], b: DenseVector[Double]): DenseVector[Double] = {

    require(a.rows == a.cols)
    require(a.rows == b.length)

    val x = DenseVector(b.toArray)

    val info: Int = {
      val info = new intW(0)
      LAPACK
        .getInstance()
        .dtrtrs("U", "N", "N", a.rows, 1, a.toArray, a.rows, x.data, x.length, info) // x := A \ x
      info.`val`
    }

    if (info > 0) {
      throw new MatrixSingularException()
    } else if (info < 0) {
      throw new IllegalArgumentException()
    }

    x
  }
}
