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

import breeze.linalg.DenseVector
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.util.FastMath
import org.apache.spark.sql.catalyst.InternalRow

import io.projectglow.common.GlowLogging

case class RegressionStats(beta: Double, standardError: Double, pValue: Double)

object LinearRegressionGwas extends GlowLogging {

  /**
   * Fits a linear regression model to a single variant.
   *
   * The algorithm here is based off the linear regression algorithm used in Hail,
   * as described in https://arxiv.org/pdf/1901.09531.pdf.
   *
   * At a high level, we compute the QR decomposition once per covariate matrix, and use Q
   * to project the genotypes into the orthogonal complement of the column space of the covariate
   * matrix. Then we use a simple algorithm for linear regression with one independent variable
   * to solve for relevant output (https://en.wikipedia.org/wiki/Simple_linear_regression#Numerical_example).
   */
  def runRegression(
      genotypes: DenseVector[Double],
      phenotypes: DenseVector[Double],
      covariateQRContext: CovariateQRContext): RegressionStats = {
    require(
      genotypes.length == phenotypes.length,
      "Number of samples differs between genotype and phenotype arrays")
    require(
      covariateQRContext.covQt.cols == genotypes.length,
      "Number of samples differs between genotype array and covariate matrix")

    val qtx = covariateQRContext.covQt * genotypes
    val qty = covariateQRContext.covQt * phenotypes

    val xdoty = (phenotypes dot genotypes) - (qty dot qtx)
    val xdotx = (genotypes dot genotypes) - (qtx dot qtx)
    val ydoty = (phenotypes dot phenotypes) - (qty dot qty)
    val beta = xdoty / xdotx
    val standardError =
      FastMath.sqrt((ydoty / xdotx - beta * beta) / covariateQRContext.degreesOfFreedom)

    // t-statistic
    val t = beta / standardError
    val tDist = new TDistribution(covariateQRContext.degreesOfFreedom)
    val pvalue = 2 * tDist.cumulativeProbability(-Math.abs(t))
    RegressionStats(beta, standardError, pvalue)
  }

  def linearRegressionGwas(
      genotypes: DenseVector[Double],
      phenotypes: DenseVector[Double],
      covariateQR: CovariateQRContext): InternalRow = {

    val regressionStats = runRegression(genotypes, phenotypes, covariateQR)

    InternalRow(regressionStats.beta, regressionStats.standardError, regressionStats.pValue)
  }
}
