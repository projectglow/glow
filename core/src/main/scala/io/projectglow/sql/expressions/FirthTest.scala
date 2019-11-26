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
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.netlib.util.intW

object FirthTest extends LogitTestWithoutNullFit {
  override type FitState = FirthFitState
  override val resultSchema: StructType = Encoders.product[LogitTestResults].schema

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
      return LogitTestResults.nanRow
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
case class FirthFitState(
    b: DenseVector[Double],
    logLkhd: Double,
    fisher: DenseMatrix[Double],
    converged: Boolean,
    exploded: Boolean)
