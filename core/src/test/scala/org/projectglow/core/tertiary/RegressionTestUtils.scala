package org.projectglow.core.tertiary

import breeze.linalg.{DenseMatrix => BreezeDenseMatrix}
import org.apache.spark.ml.linalg.{DenseMatrix => SparkDenseMatrix}

case class RegressionRow(
    genotypes: Array[Double],
    phenotypes: Array[Double],
    covariates: SparkDenseMatrix)

case class TestData(
    genotypes: Seq[Seq[Double]],
    phenotypes: Seq[Double],
    covariates: Seq[Array[Double]])

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
}
