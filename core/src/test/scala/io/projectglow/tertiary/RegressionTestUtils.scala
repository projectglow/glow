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

package io.projectglow.tertiary

import breeze.linalg.{DenseMatrix => BreezeDenseMatrix}
import org.apache.spark.ml.linalg.{DenseMatrix => SparkDenseMatrix}

case class RegressionRow(
    genotypes: Array[Double],
    phenotypes: Array[Double],
    covariates: SparkDenseMatrix)

case class TestData(
    genotypes: Seq[Seq[Double]],
    phenotypes: Seq[Double],
    covariates: Seq[Array[Double]],
    offsetOption: Option[Seq[Double]])

object RegressionTestUtils {
  def twoDArrayToSparkMatrix(input: Array[Array[Double]]): SparkDenseMatrix = {
    new SparkDenseMatrix(
      input.length,
      input.head.length,
      input(0).indices.flatMap(i => input.map(_(i))).toArray)
  }

  def twoDArrayToBreezeMatrix(input: Array[Array[Double]]): BreezeDenseMatrix[Double] = {
    new BreezeDenseMatrix(
      input.length,
      input.head.length,
      input(0).indices.flatMap(i => input.map(_(i))).toArray)
  }

  def testDataToRows(testData: TestData): Seq[RegressionRow] = {
    testData.genotypes.map { g =>
      RegressionRow(
        g.toArray,
        testData.phenotypes.toArray,
        twoDArrayToSparkMatrix(testData.covariates.toArray))
    }
  }
}
