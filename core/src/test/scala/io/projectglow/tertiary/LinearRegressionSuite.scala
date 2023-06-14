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

import breeze.linalg.DenseVector
import io.projectglow.sql.GlowBaseTest
import io.projectglow.sql.expressions.{CovariateQRContext, LinearRegressionGwas, RegressionStats}
import io.projectglow.tertiary.RegressionTestUtils._
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.linear.SingularMatrixException
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.spark.SparkException
import org.apache.spark.sql.functions._

import scala.concurrent.duration._
import scala.util.Random

class LinearRegressionSuite extends GlowBaseTest {

  lazy val sess = spark
  lazy val random = {
    val seed = System.currentTimeMillis()
    logger.info(s"Using random seed $seed")
    new Random(seed)
  }

  def runRegression(
      genotypes: Array[Double],
      phenotypes: Array[Double],
      covariates: Array[Array[Double]]): RegressionStats = {

    val phenotypeVector = new DenseVector[Double](phenotypes)
    val genotypeVector = new DenseVector[Double](genotypes)
    val covariateQR =
      CovariateQRContext.computeQR(twoDArrayToSparkMatrix(covariates))
    LinearRegressionGwas.runRegression(genotypeVector, phenotypeVector, covariateQR)
  }

  def testDataToOlsBaseline(testData: TestData): Seq[RegressionStats] = {
    testData.genotypes.map { g =>
      olsBaseline(g.toArray, testData.phenotypes.toArray, testData.covariates.toArray)
    }
  }

  def olsBaseline(
      genotypes: Array[Double],
      phenotypes: Array[Double],
      covariates: Array[Array[Double]]): RegressionStats = {

    // transform the data in to design matrix and y matrix compatible with OLSMultipleLinearRegression
    val observationLength = covariates(0).length + 1
    val numObservations = genotypes.length
    val x = new Array[Array[Double]](numObservations)

    // iterate over observations, copying correct elements into sample array and filling the x matrix.
    // the first element of each sample in x is the coded genotype and the rest are the covariates.
    var sample = new Array[Double](observationLength)
    for (i <- 0 until numObservations) {
      sample = new Array[Double](observationLength)
      for (j <- covariates(i).indices) {
        sample(j) = covariates(i)(j)
      }
      sample(sample.length - 1) = genotypes(i)
      x(i) = sample
    }

    // create linear model
    val ols = new OLSMultipleLinearRegression()
    ols.setNoIntercept(true) // We manually pass in the intercept

    // input sample data
    ols.newSampleData(phenotypes, x)

    // calculate coefficients
    val beta = ols.estimateRegressionParameters().last

    // compute the regression parameters standard errors
    val genoSE = ols.estimateRegressionParametersStandardErrors().last

    // test statistic t for jth parameter is equal to bj/SEbj, the parameter estimate divided by its standard error
    val t = beta / genoSE

    /* calculate p-value and report:
     Under null hypothesis (i.e. the j'th element of weight vector is 0) the relevant distribution is
     a t-distribution with N-p-1 degrees of freedom.
     */
    val tDist = new TDistribution(numObservations - observationLength)
    val pvalue = 2 * tDist.cumulativeProbability(-Math.abs(t))

    RegressionStats(beta, genoSE, pvalue)
  }

  def generateTestData(
      nSamples: Int,
      nVariants: Int,
      nRandomCovariates: Int,
      includeIntercept: Boolean = true,
      multiplier: Int = 1): TestData = {
    val genotypes = Range(0, nVariants).map { _ =>
      Range(0, nSamples).map(_ => multiplier * random.nextDouble()).toArray
    }.toArray

    val phenotypes = Range(0, nSamples).map(_ => multiplier * random.nextDouble()).toArray
    val nCovariates = if (includeIntercept) nRandomCovariates + 1 else nRandomCovariates
    val covariates = Range(0, nSamples).map(_ => new Array[Double](nCovariates)).toArray
    val startIdx = if (includeIntercept) {
      covariates.foreach(_(0) = 1)
      1
    } else {
      0
    }

    Range(startIdx, nCovariates).foreach { i =>
      covariates.foreach(_(i) = multiplier * random.nextDouble())
    }
    TestData(genotypes, phenotypes, covariates, None)
  }

  def timeIt[T](opName: String)(f: => T): T = {
    val start = System.nanoTime()
    val ret = f
    val end = System.nanoTime()
    logger.info(s"Completed '$opName' in ${(end - start).nanos.toMillis}ms")
    ret
  }

  def compareRegressionStats(s1: RegressionStats, s2: RegressionStats): Unit = {
    assert(s1.beta ~== s2.beta relTol 0.02)
    assert(s1.standardError ~== s2.standardError relTol 0.02)
    assert(s1.pValue ~== s2.pValue relTol 0.02)
  }

  def compareToApacheOLS(testData: TestData, useSpark: Boolean): Unit = {
    import sess.implicits._
    val apacheResults = timeIt("Apache linreg") {
      testDataToOlsBaseline(testData)
    }

    val ourResults = timeIt("DB linreg") {
      if (useSpark) {
        val rows = testDataToRows(testData)
        // Add id to preserve sorting
        spark
          .createDataFrame(rows)
          .withColumn("id", monotonically_increasing_id())
          .repartition(20)
          .withColumn("linreg", expr("linear_regression_gwas(genotypes, phenotypes, covariates)"))
          .orderBy("id")
          .selectExpr("expand_struct(linreg)")
          .as[RegressionStats]
          .collect()
          .toSeq
      } else {
        val gwasContext =
          CovariateQRContext.computeQR(twoDArrayToSparkMatrix(testData.covariates))
        val phenotypes = new DenseVector[Double](testData.phenotypes)
        testData
          .genotypes
          .map { g =>
            val genotypes = new DenseVector[Double](g)
            LinearRegressionGwas.runRegression(genotypes, phenotypes, gwasContext)
          }
          .toSeq
      }
    }

    apacheResults.zip(ourResults).foreach {
      case (ols, db) => compareRegressionStats(ols, db)
    }
  }

  // The cars dataset built into R
  val cars: TestData = {
    val s =
      """
        |   speed dist
        |1      4    2
        |2      4   10
        |3      7    4
        |4      7   22
        |5      8   16
        |6      9   10
        |7     10   18
        |8     10   26
        |9     10   34
        |10    11   17
        |11    11   28
        |12    12   14
        |13    12   20
        |14    12   24
        |15    12   28
        |16    13   26
        |17    13   34
        |18    13   34
        |19    13   46
        |20    14   26
        |21    14   36
        |22    14   60
        |23    14   80
        |24    15   20
        |25    15   26
        |26    15   54
        |27    16   32
        |28    16   40
        |29    17   32
        |30    17   40
        |31    17   50
        |32    18   42
        |33    18   56
        |34    18   76
        |35    18   84
        |36    19   36
        |37    19   46
        |38    19   68
        |39    20   32
        |40    20   48
        |41    20   52
        |42    20   56
        |43    20   64
        |44    22   66
        |45    23   54
        |46    24   70
        |47    24   92
        |48    24   93
        |49    24  120
        |50    25   85
      """.stripMargin
    val parsed = s
      .trim()
      .split("\n")
      .drop(1)
      .map(l => l.split("\\s+").drop(1).map(_.toDouble))
    val genotypes = parsed.map(_(0))
    val phenotypes = parsed.map(_(1))
    val covariates = genotypes.map(_ => Array(1d))
    TestData(Array(genotypes), phenotypes, covariates, None)
  }

  test("against R glm") {
    val golden = RegressionStats(3.932d, 0.4155d, 1.49e-12)
    compareRegressionStats(
      golden,
      runRegression(cars.genotypes.head.toArray, cars.phenotypes.toArray, cars.covariates.toArray))
  }

  // Sanity test to make sure that our OLS baseline is correct
  test("against R glm (Apache OLS)") {
    val golden = RegressionStats(3.932d, 0.4155d, 1.49e-12)
    compareRegressionStats(
      golden,
      olsBaseline(cars.genotypes.head.toArray, cars.phenotypes.toArray, cars.covariates.toArray))
  }

  test("intercept only, 1 site") {
    val testData = generateTestData(10, 1, 0, true, 1)
    compareToApacheOLS(testData, false)
  }

  test("intercept only, many sites") {
    val testData = generateTestData(10, 100, 0, true, 1)
    compareToApacheOLS(testData, false)
  }

  test("many covariates, many sites") {
    val testData = generateTestData(30, 100, 26, true, 1)
    compareToApacheOLS(testData, false)
  }

  test("many covariates, many sites, with spark") {
    val testData = generateTestData(30, 100, 26, true, 1)
    compareToApacheOLS(testData, true)
  }

  test("multiple phenotypes") {
    import sess.implicits._

    val testData = generateTestData(30, 1, 1, true, 1)
    val testData2 = testData.copy(phenotypes = testData.phenotypes.map(_ => Random.nextDouble()))
    val rows = testDataToRows(testData) ++ testDataToRows(testData2)
    val results = (spark
      .createDataFrame(rows)
      .withColumn("id", monotonically_increasing_id())
      .withColumn("linreg", expr("linear_regression_gwas(genotypes, phenotypes, covariates)"))
      .orderBy("id")
      .selectExpr("expand_struct(linreg)")
      .as[RegressionStats]
    ).collect
    assert(results.size == 2)
    results.zip(testDataToOlsBaseline(testData) ++ testDataToOlsBaseline(testData2)).foreach {
      case (stats1, stats2) => compareRegressionStats(stats1, stats2)
    }
  }

  def checkIllegalArgumentException(rows: Seq[RegressionRow], error: String): Unit = {
    import io.projectglow.functions._
    val e = intercept[java.lang.IllegalArgumentException] {
      spark
        .createDataFrame(rows)
        .withColumn(
          "linreg",
          linear_regression_gwas(col("genotypes"), col("phenotypes"), col("covariates")))
        .collect
    }
  }

  // Our linear regression algorithm projects the genotypes onto orthogonal complement of the
  // covariate vector space. When the genotypes are a linear combination of some covariates, this
  // projection is 0, so beta, standard error, and p value are all undefined. However, finite
  // precision can instead cause the error to be massive with respect to beta.
  test("large or NaN p value if genotypes are in covariate span") {
    val testData = generateTestData(30, 1, 1, true, 1)
    val genotypes = twoDArrayToBreezeMatrix(testData.covariates.toArray)(::, 1)
    val phenotypes = new DenseVector[Double](testData.phenotypes.toArray)
    val ctx = CovariateQRContext.computeQR(twoDArrayToSparkMatrix(testData.covariates.toArray))

    assertThrows[SingularMatrixException] {
      olsBaseline(genotypes.toArray, phenotypes.toArray, testData.covariates.toArray)
    }

    val results = LinearRegressionGwas.runRegression(genotypes, phenotypes, ctx)
    assert(results.pValue > 0.999999 || results.pValue.isNaN)
  }

  test("throws exception if more covariates than samples") {
    val testData = generateTestData(26, 100, 30, true, 1)
    val rows = testDataToRows(testData)
    checkIllegalArgumentException(rows, "Number of covariates must be less than number of samples")
  }

  test("throws exception if number of genotypes and phenotypes do not match") {
    val testData = generateTestData(10, 1, 0, true, 1)
    val rows = testData.genotypes.map { g =>
      RegressionRow(g.tail, testData.phenotypes, twoDArrayToSparkMatrix(testData.covariates), None)
    }
    checkIllegalArgumentException(
      rows,
      "Number of samples differs between genotype and phenotype arrays")
  }

  test("throws exception if number of samples does not match between genotypes and covariates") {
    val testData = generateTestData(10, 1, 0, true, 1)
    val rows = testData.genotypes.map { g =>
      RegressionRow(
        g.tail,
        testData.phenotypes.tail,
        twoDArrayToSparkMatrix(testData.covariates),
        None)
    }
    checkIllegalArgumentException(
      rows,
      "Number of samples differs between genotype array and covariate matrix")
  }
}
