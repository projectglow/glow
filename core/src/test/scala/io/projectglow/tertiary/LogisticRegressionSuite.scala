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

import java.io.File

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, Encoders, SQLUtils}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}
import org.apache.spark.sql.types.StructType

import io.projectglow.SparkShim
import io.projectglow.sql.GlowBaseTest
import io.projectglow.sql.expressions.{LikelihoodRatioTest, LogisticRegressionGwas, LogitTestResults, NewtonResult}
import io.projectglow.tertiary.RegressionTestUtils._

class LogisticRegressionSuite extends GlowBaseTest {

  private lazy val sess = spark

  private def runLRT(testData: TestData, onSpark: Boolean): Seq[LogitTestResults] = {
    runTest("LRT", testData, onSpark)
  }

  private def runTest(
      logitTest: String,
      testData: TestData,
      onSpark: Boolean): Seq[LogitTestResults] = {
    import sess.implicits._
    if (onSpark) {
      val rows = testData.genotypes.map { g =>
        RegressionRow(
          g.toArray,
          testData.phenotypes.toArray,
          twoDArrayToSparkMatrix(testData.covariates.toArray))
      }

      spark
        .createDataFrame(rows)
        .withColumn("id", monotonically_increasing_id())
        .repartition(10)
        .withColumn(
          "logit",
          expr(s"logistic_regression_gwas(genotypes, phenotypes, covariates, '$logitTest')"))
        .orderBy("id")
        .selectExpr("expand_struct(logit)")
        .as[LogitTestResults]
        .collect()
        .toSeq
    } else {
      val covariatesMatrix = twoDArrayToSparkMatrix(testData.covariates.toArray)
      val t = LogisticRegressionGwas.logitTests(logitTest)
      val nullFit = t.init(testData.phenotypes.toArray, covariatesMatrix)

      val internalRows = testData.genotypes.map { g =>
        val y = new DenseVector[Double](testData.phenotypes.toArray)
        t.runTest(new DenseVector[Double](g.toArray), y, nullFit)
      }

      val schema = ScalaReflection
        .schemaFor[LogitTestResults]
        .dataType
        .asInstanceOf[StructType]

      SQLUtils
        .internalCreateDataFrame(
          spark,
          spark.sparkContext.parallelize(internalRows),
          schema,
          false
        )
        .as[LogitTestResults]
        .collect
    }
  }

  // Data from https://stats.idre.ucla.edu/stat/data/binary.csv
  // Check if fitting with (gre + gpa + rank) is significantly better than fitting with (gre + gpa)
  // R commands:
  //    df <- read.csv("https://stats.idre.ucla.edu/stat/data/binary.csv")
  //    fiftyStudents <- head(df, 50)
  //    fullFit <- glm(admit ~ gre + gpa + rank, family = binomial, fiftyStudents)
  //    nullFit <- glm(admit ~ gre + gpa, family = binomial, fiftyStudents)
  //    oddsRatio <- exp(coef(fullFit))
  //    confInt <- exp(confint.default(fullFit))
  //    lrtPValue <- anova(nullFit, fullFit, test="LRT")
  private val admitStudents: TestData = {
    val s = """
      |   admit gre  gpa rank
      |1      0 380 3.61    3
      |2      1 660 3.67    3
      |3      1 800 4.00    1
      |4      1 640 3.19    4
      |5      0 520 2.93    4
      |6      1 760 3.00    2
      |7      1 560 2.98    1
      |8      0 400 3.08    2
      |9      1 540 3.39    3
      |10     0 700 3.92    2
      |11     0 800 4.00    4
      |12     0 440 3.22    1
      |13     1 760 4.00    1
      |14     0 700 3.08    2
      |15     1 700 4.00    1
      |16     0 480 3.44    3
      |17     0 780 3.87    4
      |18     0 360 2.56    3
      |19     0 800 3.75    2
      |20     1 540 3.81    1
      |21     0 500 3.17    3
      |22     1 660 3.63    2
      |23     0 600 2.82    4
      |24     0 680 3.19    4
      |25     1 760 3.35    2
      |26     1 800 3.66    1
      |27     1 620 3.61    1
      |28     1 520 3.74    4
      |29     1 780 3.22    2
      |30     0 520 3.29    1
      |31     0 540 3.78    4
      |32     0 760 3.35    3
      |33     0 600 3.40    3
      |34     1 800 4.00    3
      |35     0 360 3.14    1
      |36     0 400 3.05    2
      |37     0 580 3.25    1
      |38     0 520 2.90    3
      |39     1 500 3.13    2
      |40     1 520 2.68    3
      |41     0 560 2.42    2
      |42     1 580 3.32    2
      |43     1 600 3.15    2
      |44     0 500 3.31    3
      |45     0 700 2.94    2
      |46     1 460 3.45    3
      |47     1 580 3.46    2
      |48     0 500 2.97    4
      |49     0 440 2.48    4
      |50     0 400 3.35    3
    """.stripMargin

    val parsed = s
      .trim()
      .split("\n")
      .drop(1)
      .map(l => l.split("\\s+").drop(1).map(_.toDouble))
    val phenotypes = parsed.map(_(0))
    val covariates = parsed.map { r =>
      Array(1d, r(1), r(2))
    }
    val genotypes = parsed.map(_(3))
    TestData(Seq(genotypes), phenotypes, covariates)
  }

  private val admitStudentsStats =
    LogitTestResults(-0.611263, 0.54266503, Seq(2.901759e-01, 1.014851), 0.04693173)

  private val interceptOnlyV1 = TestData(
    Seq(Seq(0, 1, 2, 0, 0, 1)),
    Seq(0, 0, 1, 1, 1, 1),
    Seq(Array(1), Array(1), Array(1), Array(1), Array(1), Array(1)))
  private val interceptOnlyV1Stats =
    LogitTestResults(0.4768, 1.610951, Seq(0.1403952, 18.48469), 0.6935)
  private val interceptOnlyV1FirthStats =
    LogitTestResults(
      0.2434646,
      Math.exp(0.2434646),
      Seq(-2.020431, 2.507360).map(Math.exp),
      0.796830)

  private val interceptOnlyV2 = interceptOnlyV1.copy(phenotypes = Seq(0, 0, 1, 0, 1, 1))
  private val interceptOnlyV2Stats =
    LogitTestResults(1.4094, 4.0936366, Seq(0.26608762, 62.978730), 0.2549)
  private val interceptOnlyV2FirthStats =
    LogitTestResults(
      0.8731197,
      Math.exp(0.8731197),
      Seq(-1.495994, 3.242234).map(Math.exp),
      0.3609153)

  private val gtsAndCovariates = TestData(
    Seq(Seq(0, 1, 2, 0, 0, 1)),
    Seq(0, 0, 1, 1, 1, 1),
    Seq(
      Array(1, 0, -1),
      Array(1, 2, 3),
      Array(1, 1, 5),
      Array(1, -2, 0),
      Array(1, -2, -4),
      Array(1, 4, 3)))
  private val gtsAndCovariatesStats =
    LogitTestResults(3.1776, 23.9884595, Seq(0.007623126, 75486.900886), 0.35)

  // https://en.wikipedia.org/wiki/Separation_(statistics)
  private val completeSeparation = TestData(
    Seq(Seq(0, 1, 2, 0, 0, 1)),
    Seq(0, 0, 1, 1, 1, 1),
    Seq(Array(0), Array(0), Array(1), Array(1), Array(1), Array(1)))

  private val linearlyDependentCovariates = TestData(
    Seq(Seq(0, 1, 2, 0, 0, 1)),
    Seq(0, 0, 1, 1, 1, 1),
    Seq(Array(0, 0), Array(1, 2), Array(1, 2), Array(1, 2), Array(1, 2), Array(1, 2)))

  case class TestDataAndGoldenStats(testData: TestData, lrtStats: LogitTestResults)

  private val TEST_DATA_AND_GOLDEN_STATS = Seq(
    TestDataAndGoldenStats(admitStudents, admitStudentsStats),
    TestDataAndGoldenStats(interceptOnlyV1, interceptOnlyV1Stats),
    TestDataAndGoldenStats(interceptOnlyV2, interceptOnlyV2Stats),
    TestDataAndGoldenStats(gtsAndCovariates, gtsAndCovariatesStats)
  )

  private def compareLogitTestResults(s1: LogitTestResults, s2: LogitTestResults): Unit = {

    assert(s1.beta ~== s2.beta relTol 0.02)
    assert(s1.oddsRatio ~== s2.oddsRatio relTol 0.02)
    assert(s1.waldConfidenceInterval.length == 2)
    assert(s2.waldConfidenceInterval.length == 2)
    s1.waldConfidenceInterval.zip(s2.waldConfidenceInterval).foreach {
      case (s1ci, s2ci) =>
        assert(s1ci ~== s2ci relTol 0.02)
    }
    assert(s1.pValue ~== s2.pValue relTol 0.02)
  }

  private def runNewtonIterations(testData: TestData): NewtonResult = {
    val fitState = LikelihoodRatioTest.init(
      testData.phenotypes.toArray,
      twoDArrayToSparkMatrix(testData.covariates.toArray))
    fitState.nullFit
  }

  test("Does not converge in case of complete separation") {
    val nonConvergedNewton = runNewtonIterations(completeSeparation)
    assert(!nonConvergedNewton.converged)
    assert(!nonConvergedNewton.exploded)
    assert(nonConvergedNewton.nIter == 26)
  }

  test("Explodes in case of linearly dependent covariates") {
    val nonConvergedNewton = runNewtonIterations(linearlyDependentCovariates)
    assert(!nonConvergedNewton.converged)
    assert(nonConvergedNewton.exploded)
    assert(nonConvergedNewton.nIter == 1)
  }

  test("Throw error if test is not foldable") {
    val rows = admitStudents.genotypes.map { g =>
      RegressionRow(
        g.toArray,
        admitStudents.phenotypes.toArray,
        twoDArrayToSparkMatrix(admitStudents.covariates.toArray))
    }
    val e = intercept[AnalysisException] {
      spark
        .createDataFrame(rows)
        .repartition(10)
        .withColumn("test", monotonically_increasing_id())
        .withColumn(
          "logit",
          expr("logistic_regression_gwas(genotypes, phenotypes, covariates, test)"))
        .collect()
    }
    assert(e.getMessage.contains("Test must be a constant value"))
  }

  test("Throw error if test is not supported") {
    val rows = admitStudents.genotypes.map { g =>
      RegressionRow(
        g.toArray,
        admitStudents.phenotypes.toArray,
        twoDArrayToSparkMatrix(admitStudents.covariates.toArray))
    }
    val ex = intercept[IllegalArgumentException] {
      spark
        .createDataFrame(rows)
        .withColumn(
          "logit",
          expr("logistic_regression_gwas(genotypes, phenotypes, covariates, 'fakeTest')"))
        .selectExpr("expand_struct(logit)")
        .collect()
    }
    assert(ex.getMessage.toLowerCase.contains("supported tests are currently"))
  }

  Seq(true, false).foreach { onSpark =>
    gridTest(s"Likelihood ratio test against R (on Spark: $onSpark)")(TEST_DATA_AND_GOLDEN_STATS) {
      testCase =>
        val ourStats = runLRT(testCase.testData, onSpark).head
        compareLogitTestResults(testCase.lrtStats, ourStats)
    }
  }

  def checkAllNan(lrtStats: LogitTestResults): Unit = {
    assert(lrtStats.beta.isNaN)
    assert(lrtStats.oddsRatio.isNaN)
    lrtStats.waldConfidenceInterval.foreach { c =>
      assert(c.isNaN)
    }
    assert(lrtStats.pValue.isNaN)
  }

  test("Return NaNs if null fit didn't converge") {
    val ourStats = runLRT(completeSeparation, false).head
    checkAllNan(ourStats)
  }

  test("Return NaNs if full fit didn't converge") {
    val ourStats = runLRT(
      TestData(
        Seq(completeSeparation.covariates.flatten),
        completeSeparation.phenotypes,
        completeSeparation.genotypes.head.map(Array(_))),
      false).head
    checkAllNan(ourStats)
  }

  private val allLogitTests = LogisticRegressionGwas.logitTests.keys.toSeq
  gridTest("Check sample number matches between phenos and covars")(allLogitTests) { testName =>
    val fewerPhenoSamples = TestData(
      Seq(Seq(0, 1, 2, 0, 0)),
      Seq(0, 0, 1, 1, 1),
      Seq(Array(1), Array(1), Array(1), Array(1), Array(1), Array(1)))
    val ex = intercept[SparkException] {
      runTest(testName, fewerPhenoSamples, true)
    }
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
    assert(
      ex.getCause
        .getMessage
        .contains("Number of samples do not match between phenotype vector and covariate matrix"))
  }

  gridTest("Check sample number matches between genos and phenos")(allLogitTests) { testName =>
    val fewerPhenoSamples = TestData(
      Seq(Seq(0, 1, 2, 0, 0)),
      Seq(0, 0, 1, 1, 1, 1),
      Seq(Array(1), Array(1), Array(1), Array(1), Array(1), Array(1)))
    val ex = intercept[SparkException] {
      runTest(testName, fewerPhenoSamples, true)
    }
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
    assert(
      ex.getCause
        .getMessage
        .contains("Number of samples differs between genotype and phenotype arrays"))
  }

  gridTest("Checks for non-zero covariates")(allLogitTests) { testName =>
    val fewerPhenoSamples = TestData(
      Seq(Seq(0, 1, 2, 0, 0, 1)),
      Seq(0, 0, 1, 1, 1, 1),
      Seq(Array.empty, Array.empty, Array.empty, Array.empty, Array.empty, Array.empty))

    val ex = intercept[SparkException] {
      runLRT(fewerPhenoSamples, true)
    }
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
    assert(ex.getMessage.toLowerCase.contains("must have at least one column"))
  }

  test("Run multiple regressions") {
    import sess.implicits._

    val rows = testDataToRows(interceptOnlyV1) ++ testDataToRows(interceptOnlyV2)

    val ourStats = spark
      .createDataFrame(rows)
      .withColumn("id", monotonically_increasing_id())
      .repartition(10)
      .withColumn(
        "logit",
        expr("logistic_regression_gwas(genotypes, phenotypes, covariates, 'LRT')"))
      .orderBy("id")
      .selectExpr("expand_struct(logit)")
      .as[LogitTestResults]
      .collect()
      .toSeq

    Seq(interceptOnlyV1Stats, interceptOnlyV2Stats).zip(ourStats).foreach {
      case (golden, our) =>
        compareLogitTestResults(golden, our)
    }
  }

  gridTest("firth regression vs R onSpark=")(Seq(true, false)) { onSpark =>
    val s = FileUtils.readFileToString(new File(s"$testDataHome/r/sex2.txt"))
    val parsed = s
      .trim()
      .split("\n")
      .drop(1)
      .map(l => l.split("\\s+").map(_.toDouble))
    val phenotypes = parsed.map(_(0))
    val covariates = parsed.map { r =>
      1d +: r.tail.init
    }
    val genotypes = parsed.map(_.last)
    val testData = TestData(Seq(genotypes), phenotypes, covariates)
    val ours = runTest("firth", testData, onSpark).head
    // golden stats are from R's logistf package
    // data(sex2)
    // fit<-logistf(case ~ age+oc+vic+vicl+vis+dia, data=sex2, pl=TRUE)
    // summary(fit)
    // To get Wald confidence intervals, set pl=FALSE
    val golden = LogitTestResults(
      3.09601263,
      Math.exp(3.09601263),
      Seq(-0.1869446, 6.37896984).map(Math.exp),
      4.951873e-03
    )
    compareLogitTestResults(golden, ours)
  }

  test("multiple firth regressions") {
    import sess.implicits._
    val rows = testDataToRows(interceptOnlyV1) ++ testDataToRows(interceptOnlyV2)

    val ourStats = spark
      .createDataFrame(rows)
      .withColumn("id", monotonically_increasing_id())
      .repartition(10)
      .withColumn(
        "logit",
        expr("logistic_regression_gwas(genotypes, phenotypes, covariates, 'firth')"))
      .orderBy("id")
      .selectExpr("expand_struct(logit)")
      .as[LogitTestResults]
      .collect()
      .toSeq

    Seq(interceptOnlyV1FirthStats, interceptOnlyV2FirthStats).zip(ourStats).foreach {
      case (golden, our) =>
        compareLogitTestResults(golden, our)
    }
  }

  test("firth returns nan if model can't be fit") {
    val result = runTest("firth", linearlyDependentCovariates, onSpark = false)
    checkAllNan(result.head)
  }

  test("tests are case insensitive") {
    assert(
      LogisticRegressionGwas.logitTests.get("firth") ==
      LogisticRegressionGwas.logitTests.get("FIRTH"))
    assert(LogisticRegressionGwas.logitTests.get("monkey").isEmpty)
  }
}
