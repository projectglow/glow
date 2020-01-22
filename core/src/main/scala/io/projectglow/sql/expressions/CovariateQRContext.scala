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

import breeze.linalg.{qr, DenseMatrix, DenseVector}
import org.apache.spark.ml.linalg.{DenseMatrix => SparkDenseMatrix}

import io.projectglow.common.GlowLogging

/**
 * Context that can be computed once for all variant sites for a linear regression GWAS analysis.
 */
case class CovariateQRContext(covQt: DenseMatrix[Double], degreesOfFreedom: Int)

object CovariateQRContext extends GlowLogging {
  def computeQR(covariateMatrix: SparkDenseMatrix): CovariateQRContext = {

    require(
      covariateMatrix.numRows > covariateMatrix.numCols,
      s"Number of covariates must be less than number of samples")

    val covM = new DenseMatrix[Double](
      covariateMatrix.numRows,
      covariateMatrix.numCols,
      covariateMatrix.values)
    val q = qr.reduced.justQ(covM)
    val covQt = q.t

    CovariateQRContext(covQt, covariateMatrix.numRows - covariateMatrix.numCols - 1)
  }
}
