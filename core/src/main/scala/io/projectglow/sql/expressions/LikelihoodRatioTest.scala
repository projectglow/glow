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

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.ml.linalg.{DenseMatrix => SparkDenseMatrix}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

object LikelihoodRatioTest extends LogitTest {
  override type FitState = LRTFitState
  override def canReuseNullFit: Boolean = true
  override val resultSchema: StructType = Encoders.product[LogitTestResults].schema

  override def init(phenotypes: Array[Double], covariates: SparkDenseMatrix): LRTFitState = {
    val nullX = new DenseMatrix(covariates.numRows, covariates.numCols, covariates.values)
    val y = new DenseVector(phenotypes)
    val nullFitState = new NewtonIterationsState(covariates.numRows, covariates.numCols)
    NewtonIterationsState.initFromMatrix(nullFitState, nullX, y)
    val nullFit = LogisticRegressionGwas.newtonIterations(nullX, y, nullFitState)
    val fullFitState = new NewtonIterationsState(covariates.numRows, covariates.numCols + 1)
    val x = DenseMatrix.horzcat(nullX, DenseMatrix.zeros[Double](covariates.numRows, 1))
    LRTFitState(x, nullFit, fullFitState)
  }

  override def runTest(
      genotypes: DenseVector[Double],
      phenotypes: DenseVector[Double],
      fitState: LRTFitState): InternalRow = {
    fitState.x(::, -1) := genotypes
    NewtonIterationsState.initFromMatrixAndNullFit(
      fitState.placeholderState,
      fitState.x,
      phenotypes,
      fitState.nullFit.args)

    val fullFit =
      LogisticRegressionGwas.newtonIterations(fitState.x, phenotypes, fitState.placeholderState)

    if (!fitState.nullFit.converged || !fullFit.converged) {
      return LogitTestResults.nanRow
    }

    val beta = fullFit.args.b(-1)
    LogisticRegressionGwas.makeStats(
      beta,
      fullFit.args.fisher,
      fullFit.logLkhd,
      fitState.nullFit.logLkhd)
  }
}
