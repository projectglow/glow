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

import breeze.linalg.{*, diag, max, qr, sum, DenseMatrix, DenseVector, MatrixSingularException}
import breeze.numerics.{abs, log, sigmoid, sqrt}
import com.github.fommil.netlib.LAPACK
import org.apache.spark.ml.linalg.{DenseMatrix => SparkDenseMatrix}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.netlib.util.intW

object FirthTest extends LogitTest {
  override def fitStatePerPhenotype: Boolean = false
  override type FitState = FirthFitState
  override val resultSchema: StructType = Encoders.product[LogitTestResults].schema

  override def init(
      phenotypes: Array[Double],
      covariates: SparkDenseMatrix,
      offsetOption: Option[Array[Double]]): FirthFitState = {

    val covariateX =
      new DenseMatrix[Double](covariates.numRows, covariates.numCols, covariates.values)
    FirthFitState(
      DenseMatrix.horzcat(covariateX, DenseMatrix.zeros[Double](covariateX.rows, 1)),
      new FirthNewtonArgs(covariates.numRows, covariates.numCols),
      new FirthNewtonArgs(covariates.numRows, covariates.numCols + 1)
    )
  }

  override def runTest(
      genotypes: DenseVector[Double],
      phenotypes: DenseVector[Double],
      offsetOption: Option[DenseVector[Double]],
      fitState: FirthFitState): InternalRow = {

    fitState.x(::, -1) := genotypes
    fitState.nullFitArgs.clear()
    val nullFit = fitFirth(fitState.x, phenotypes, offsetOption, fitState.nullFitArgs)

    if (!nullFit.converged) {
      return LogitTestResults.nanRow
    }

    fitState.fullFitArgs.clear()
    fitState.fullFitArgs.initFromNullFit(nullFit.fitState)
    val fullFit = fitFirth(fitState.x, phenotypes, offsetOption, fitState.fullFitArgs)

    if (!fullFit.converged) {
      return LogitTestResults.nanRow
    }

    val beta = fullFit.fitState.b(-1)
    val fisher = fitState
        .x
        .t * (fitState.x(::, *) *:* (fullFit.fitState.mu *:* (1d - fullFit.fitState.mu)))
    LogisticRegressionGwas.makeStats(beta, fisher, fullFit.logLkhd, nullFit.logLkhd)
  }

  // Adapted from Hail's `fitFirth` method.
  def fitFirth(
      x: DenseMatrix[Double],
      y: DenseVector[Double],
      offsetOption: Option[DenseVector[Double]],
      args: FirthNewtonArgs,
      maxIter: Int = 100,
      tol: Double = 1e-6): FirthFit = {

    val m0 = args.b.length
    var logLkhd = 0d
    var iter = 1
    var converged = false
    var exploded = false

    while (!converged && !exploded && iter <= maxIter) {
      try {
        val eta = offsetOption match {
          case Some(offset) => offset + x(::, 0 until m0) * args.b
          case None => x(::, 0 until m0) * args.b
        }
        args.mu := sigmoid(eta)
        args.sqrtW := sqrt(args.mu *:* (1d - args.mu))
        val QR = qr.reduced(x(::, *) *:* args.sqrtW)
        val h = QR.q(*, ::).map(r => r dot r)
        val deltaB = solveUpperTriangular(
          QR.r(0 until m0, 0 until m0),
          QR.q(::, 0 until m0).t * (((y - args.mu) + (h *:* (0.5 - args.mu))) /:/ args.sqrtW))

        if (deltaB(0).isNaN) {
          exploded = true
        } else if (max(abs(deltaB)) < tol && iter > 1) {
          converged = true
          logLkhd = sum(breeze.numerics.log((y *:* args.mu) + ((1d - y) *:* (1d - args.mu)))) + sum(
              log(abs(diag(QR.r))))
        } else {
          iter += 1
          args.b += deltaB
        }
      } catch {
        case e: breeze.linalg.MatrixSingularException => exploded = true
        case e: breeze.linalg.NotConvergedException => exploded = true
      }
    }

    FirthFit(args, logLkhd, converged, exploded)
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

class FirthNewtonArgs(numRows: Int, numCols: Int) {
  val b = DenseVector.zeros[Double](numCols)
  val mu = DenseVector.zeros[Double](numRows)
  val sqrtW = DenseVector.zeros[Double](numRows)

  def clear(): Unit = {
    b := 0d
    mu := 0d
    sqrtW := 0d
  }

  def initFromNullFit(nullFit: FirthNewtonArgs): Unit = {
    b(0 until nullFit.b.length) := nullFit.b
    b(nullFit.b.length to -1)
  }
}

case class FirthFitState(
    // The last column of x will be rewritten with the genotypes for each new row
    x: DenseMatrix[Double],
    nullFitArgs: FirthNewtonArgs,
    fullFitArgs: FirthNewtonArgs
)

case class FirthFit(
    fitState: FirthNewtonArgs,
    logLkhd: Double,
    converged: Boolean,
    exploded: Boolean)
