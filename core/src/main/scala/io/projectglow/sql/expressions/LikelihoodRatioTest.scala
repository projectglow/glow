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

object LikelihoodRatioTest extends LogitTestWithNullFit {
  override type FitState = NewtonResult
  override val resultSchema: StructType = Encoders.product[LogitTestResults].schema

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
      return LogitTestResults.nanRow
    }

    val beta = fullFit.args.b(-1)
    LogisticRegressionGwas.makeStats(beta, fullFit.args.fisher, fullFit.logLkhd, nullFit.logLkhd)
  }
}
