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

import breeze.linalg.DenseVector
import io.projectglow.functions.{expand_struct, logistic_regression_gwas}
import io.projectglow.sql.GlowBaseTest
import io.projectglow.sql.expressions.{LikelihoodRatioTest, LogisticRegressionGwas, LogitTestResults, NewtonResult}
import io.projectglow.tertiary.RegressionTestUtils._
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, expr, monotonically_increasing_id}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, SQLUtils}

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
      val hasOffset = testData.offsetOption.isDefined
      spark
        .createDataFrame(testDataToRows(testData))
        .withColumn("id", monotonically_increasing_id())
        .repartition(10)
        .withColumn(
          "logit",
          if (hasOffset) {
            logistic_regression_gwas(
              col("genotypes"),
              col("phenotypes"),
              col("covariates"),
              logitTest,
              col("offset"))
          } else {
            logistic_regression_gwas(
              col("genotypes"),
              col("phenotypes"),
              col("covariates"),
              logitTest)
          }
        )
        .orderBy("id")
        .select(expand_struct(col("logit")))
        .as[LogitTestResults]
        .collect()
        .toSeq
    } else {
      val covariatesMatrix = twoDArrayToSparkMatrix(testData.covariates)
      val t = LogisticRegressionGwas.logitTests(logitTest)
      val nullFit = t.init(testData.phenotypes, covariatesMatrix, testData.offsetOption)

      val internalRows = testData.genotypes.map { g =>
        val y = new DenseVector[Double](testData.phenotypes)
        val offset = testData.offsetOption.map(new DenseVector[Double](_))
        t.runTest(new DenseVector[Double](g), y, offset, nullFit)
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

  /**
   * Loads test data from a text file. Phenotype is the first column. Genotype is the next to last column.
   * Offset is the last column. Other columns are covariates (an intercept is added by the function).
   * @param path
   * @return
   */
  def readTestData(path: String): TestData = {
    val s: String = FileUtils.readFileToString(new File(path))
    val parsed = s
      .trim()
      .split("\n")
      .drop(1)
      .map(l => l.split("\\s+").map(_.toDouble))

    require(parsed.length > 3, "The file should have at least 3 columns.")

    val phenotypes = parsed.map(_(0))
    val covariates = parsed.map { r =>
      1d +: r.slice(1, r.length - 2)
    }
    val genotypes = parsed.map(_.init.last)

    val offsetOption = Some(parsed.map(_.last))

    TestData(
      Array(genotypes),
      phenotypes,
      covariates,
      offsetOption
    )
  }

  private def compareLogitTestResults(s1: LogitTestResults, s2: LogitTestResults): Unit = {
    val tolerance = 0.001
    assert(s1.beta ~== s2.beta relTol tolerance)
    assert(s1.oddsRatio ~== s2.oddsRatio relTol tolerance)
    assert(s1.waldConfidenceInterval.length == 2)
    assert(s2.waldConfidenceInterval.length == 2)
    s1.waldConfidenceInterval.zip(s2.waldConfidenceInterval).foreach {
      case (s1ci, s2ci) =>
        assert(s1ci ~== s2ci relTol tolerance)
    }
    assert(s1.pValue ~== s2.pValue relTol tolerance)
  }

  private def runNewtonIterations(testData: TestData): NewtonResult = {
    val fitState = LikelihoodRatioTest.init(
      testData.phenotypes,
      twoDArrayToSparkMatrix(testData.covariates),
      testData.offsetOption)
    fitState.nullFit
  }

  // Data from https://stats.idre.ucla.edu/stat/data/binary.csv
  // We made up an offset column.
  // Check if fitting with (gre + gpa + rank) is significantly better than fitting with (gre + gpa)
  // R commands:
  //    df <- read.csv("https://stats.idre.ucla.edu/stat/data/binary.csv")
  //    fiftyStudents <- head(df, 50)
  //    # for the case with offset add the parameter offset = offset to the following two lines:
  //    fullFit <- glm(admit ~ gre + gpa + rank, family = binomial, fiftyStudents)
  //    nullFit <- glm(admit ~ gre + gpa, family = binomial, fiftyStudents) #for offset add: offset = offset
  //    oddsRatio <- exp(coef(fullFit))
  //    confInt <- exp(confint.default(fullFit))
  //    lrtPValue <- anova(nullFit, fullFit, test="LRT")

  private val admitStudentsWithOffset: TestData = readTestData(
    s"$testDataHome/r/binarywithoffset.txt")
  private val admitStudentsWithOffsetStats =
    LogitTestResults(-0.591478, 0.5535084, Seq(0.2957338280, 1.035971), 0.05590138)
  private val admitStudentsWithZeroOffset: TestData = {
    val zeroOffset = Array.fill[Double](50)(0)
    admitStudentsWithOffset.copy(offsetOption = Some(zeroOffset))
  }
  private val admitStudents: TestData = admitStudentsWithOffset.copy(offsetOption = None)
  private val admitStudentsStats =
    LogitTestResults(-0.611263, 0.54266503, Seq(2.901759e-01, 1.014851), 0.04693173)

  // Data amd golden stats are from R's logistf package
  // We made up an offset column.
  // data(sex2)
  // fit<-logistf(case ~ age+oc+vic+vicl+vis+dia, data=sex2, pl=TRUE)
  // summary(fit)
  // To get Wald confidence intervals, set pl=FALSE
  private val firthTestDataWithOffset: TestData = readTestData(
    s"$testDataHome/r/sex2withoffset.txt")
  private val firthTestDataWithOffsetStats = LogitTestResults(
    2.7397118,
    Math.exp(2.7397118),
    Seq(-0.6063838, 6.0858075).map(Math.exp),
    0.01916332
  )
  private val firthTestDataWithZeroOffset: TestData = {
    val zeroOffset = Array.fill[Double](239)(0)
    firthTestDataWithOffset.copy(offsetOption = Some(zeroOffset))
  }
  private val firthTestData: TestData = firthTestDataWithOffset.copy(offsetOption = None)
  private val firthTestDataStats = LogitTestResults(
    3.09601263,
    Math.exp(3.09601263),
    Seq(-0.1869446, 6.37896984).map(Math.exp),
    4.951873e-03
  )

  private val interceptOnlyV1 = TestData(
    Array(Array(0, 1, 2, 0, 0, 1)),
    Array(0, 0, 1, 1, 1, 1),
    Array(Array(1), Array(1), Array(1), Array(1), Array(1), Array(1)),
    None
  )
  private val interceptOnlyV1Stats =
    LogitTestResults(0.4768, 1.610951, Seq(0.1403952, 18.48469), 0.6935)
  private val interceptOnlyV1FirthStats =
    LogitTestResults(
      0.2434646,
      Math.exp(0.2434646),
      Seq(-2.020431, 2.507360).map(Math.exp),
      0.796830)

  private val interceptOnlyV2 = interceptOnlyV1.copy(phenotypes = Array(0, 0, 1, 0, 1, 1))
  private val interceptOnlyV2Stats =
    LogitTestResults(1.4094, 4.0936366, Seq(0.26608762, 62.978730), 0.2549)
  private val interceptOnlyV2FirthStats =
    LogitTestResults(
      0.8731197,
      Math.exp(0.8731197),
      Seq(-1.495994, 3.242234).map(Math.exp),
      0.3609153)

  private val interceptAndOffset = interceptOnlyV1.copy(
    phenotypes = Array(1, 0, 1, 0, 1, 1),
    offsetOption = Some(Array(0, 0, 1, 0, 1, 0)))
  private val interceptAndOffsetStats =
    LogitTestResults(0.341551, 1.40712879, Seq(0.097423, 20.323856), 0.797255)
  private val interceptAndOffsetFirthStats =
    LogitTestResults(
      0.0206165,
      Math.exp(0.0206165),
      Seq(-2.377305, 2.418538).map(Math.exp),
      0.9843376)

  private val gtsAndCovariates = TestData(
    Array(Array(0, 1, 2, 0, 0, 1)),
    Array(0, 0, 1, 1, 1, 1),
    Array(
      Array(1, 0, -1),
      Array(1, 2, 3),
      Array(1, 1, 5),
      Array(1, -2, 0),
      Array(1, -2, -4),
      Array(1, 4, 3)),
    None)

  private val gtsAndCovariatesStats =
    LogitTestResults(3.1776, 23.9884595, Seq(0.007623126, 75486.900886), 0.35)

  // https://en.wikipedia.org/wiki/Separation_(statistics)
  private val completeSeparation = TestData(
    Array(Array(0, 1, 2, 0, 0, 1)),
    Array(0, 0, 1, 1, 1, 1),
    Array(Array(0), Array(0), Array(1), Array(1), Array(1), Array(1)),
    None
  )

  private val linearlyDependentCovariates = TestData(
    Array(Array(0, 1, 2, 0, 0, 1)),
    Array(0, 0, 1, 1, 1, 1),
    Array(Array(0, 0), Array(1, 2), Array(1, 2), Array(1, 2), Array(1, 2), Array(1, 2)),
    None
  )

  case class TestDataAndGoldenStats(test: String, testData: TestData, goldenStats: LogitTestResults)

  private val TEST_DATA_AND_GOLDEN_STATS = Seq(
    TestDataAndGoldenStats("lrt", admitStudents, admitStudentsStats),
    TestDataAndGoldenStats("lrt", admitStudentsWithZeroOffset, admitStudentsStats),
    TestDataAndGoldenStats("lrt", admitStudentsWithOffset, admitStudentsWithOffsetStats),
    TestDataAndGoldenStats("lrt", interceptOnlyV1, interceptOnlyV1Stats),
    TestDataAndGoldenStats("lrt", interceptOnlyV2, interceptOnlyV2Stats),
    TestDataAndGoldenStats("lrt", gtsAndCovariates, gtsAndCovariatesStats),
    TestDataAndGoldenStats("firth", firthTestData, firthTestDataStats),
    TestDataAndGoldenStats("firth", firthTestDataWithZeroOffset, firthTestDataStats),
    TestDataAndGoldenStats("firth", firthTestDataWithOffset, firthTestDataWithOffsetStats)
  )

  Seq(true, false).foreach { onSpark =>
    gridTest(s"Our test against R (on Spark: $onSpark)")(TEST_DATA_AND_GOLDEN_STATS) { testCase =>
      val ourStats = runTest(testCase.test, testCase.testData, onSpark).head
      compareLogitTestResults(testCase.goldenStats, ourStats)
    }
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
        g,
        admitStudents.phenotypes,
        twoDArrayToSparkMatrix(admitStudents.covariates),
        None)
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
        g,
        admitStudents.phenotypes,
        twoDArrayToSparkMatrix(admitStudents.covariates),
        None)
    }
    val ex = intercept[IllegalArgumentException] {
      spark
        .createDataFrame(rows)
        .withColumn(
          "logit",
          logistic_regression_gwas(
            col("genotypes"),
            col("phenotypes"),
            col("covariates"),
            "faketest")
        )
        .select(expand_struct(col("logit")))
        .collect()
    }
    assert(ex.getMessage.toLowerCase.contains("supported tests are currently"))
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
        Array(completeSeparation.covariates.flatten),
        completeSeparation.phenotypes,
        completeSeparation.genotypes.head.map(Array(_)),
        None),
      false).head
    checkAllNan(ourStats)
  }

  private val allLogitTests = LogisticRegressionGwas.logitTests.keys.toSeq
  gridTest("Check sample number matches between phenos and covars")(allLogitTests) { testName =>
    val fewerPhenoSamples = TestData(
      Array(Array(0, 1, 2, 0, 0)),
      Array(0, 0, 1, 1, 1),
      Array(Array(1), Array(1), Array(1), Array(1), Array(1), Array(1)),
      None)
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
      Array(Array(0, 1, 2, 0, 0)),
      Array(0, 0, 1, 1, 1, 1),
      Array(Array(1), Array(1), Array(1), Array(1), Array(1), Array(1)),
      None)
    val ex = intercept[SparkException] {
      runTest(testName, fewerPhenoSamples, true)
    }
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
    assert(
      ex.getCause
        .getMessage
        .contains("Number of samples differs between genotype and phenotype arrays"))
  }

  gridTest("Check sample number matches between phenos and offset")(allLogitTests) { testName =>
    val fewerPhenoSamples = TestData(
      Array(Array(0, 1, 2, 0, 0)),
      Array(0, 0, 1, 1, 1),
      Array(Array(1), Array(1), Array(1), Array(1), Array(1)),
      Some(Array(0, 1, 0, 1, 0, 0)))
    val ex = intercept[SparkException] {
      runTest(testName, fewerPhenoSamples, true)
    }
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
    assert(
      ex.getCause
        .getMessage
        .contains("Number of samples do not match between phenotype vector and offset vector"))
  }

  gridTest("Checks covariates exist")(allLogitTests) { testName =>
    val fewerPhenoSamples = TestData(
      Array(Array(0, 1, 2, 0, 0, 1)),
      Array(0, 0, 1, 1, 1, 1),
      Array(Array.empty, Array.empty, Array.empty, Array.empty, Array.empty, Array.empty),
      None)

    val ex = intercept[SparkException] {
      runLRT(fewerPhenoSamples, true)
    }
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
    assert(ex.getMessage.toLowerCase.contains("must have at least one column"))
  }

  def runTestOnRows(logitTest: String, rows: Seq[RegressionRow]): Seq[LogitTestResults] = {
    import sess.implicits._
    spark
      .createDataFrame(rows)
      .withColumn("id", monotonically_increasing_id())
      .repartition(10)
      .withColumn(
        "logit",
        if (rows.head.offset.isDefined) {
          logistic_regression_gwas(
            col("genotypes"),
            col("phenotypes"),
            col("covariates"),
            logitTest,
            col("offset"))
        } else {
          logistic_regression_gwas(
            col("genotypes"),
            col("phenotypes"),
            col("covariates"),
            logitTest)
        }
      )
      .orderBy("id")
      .select(expand_struct(col("logit")))
      .as[LogitTestResults]
      .collect()
      .toSeq
  }

  case class TestRowsAndGoldenStats(
      test: String,
      testRows: Seq[RegressionRow],
      goldenStats: Seq[LogitTestResults])

  private val TEST_ROWS_AND_GOLDEN_STATS = Seq(
    TestRowsAndGoldenStats(
      "lrt",
      testDataToRows(interceptOnlyV1) ++ testDataToRows(interceptOnlyV2),
      Seq(interceptOnlyV1Stats, interceptOnlyV2Stats)),
    TestRowsAndGoldenStats(
      "lrt",
      testDataToRows(interceptOnlyV1.copy(offsetOption = Some(Array(0, 0, 0, 0, 0, 0)))) ++ testDataToRows(
        interceptOnlyV2.copy(offsetOption = Some(Array(0, 0, 0, 0, 0, 0)))) ++ testDataToRows(
        interceptAndOffset),
      Seq(interceptOnlyV1Stats, interceptOnlyV2Stats, interceptAndOffsetStats)
    ),
    TestRowsAndGoldenStats(
      "firth",
      testDataToRows(interceptOnlyV1) ++ testDataToRows(interceptOnlyV2),
      Seq(interceptOnlyV1FirthStats, interceptOnlyV2FirthStats)),
    TestRowsAndGoldenStats(
      "firth",
      testDataToRows(interceptOnlyV1.copy(offsetOption = Some(Array(0, 0, 0, 0, 0, 0)))) ++ testDataToRows(
        interceptOnlyV2.copy(offsetOption = Some(Array(0, 0, 0, 0, 0, 0)))) ++ testDataToRows(
        interceptAndOffset),
      Seq(interceptOnlyV1FirthStats, interceptOnlyV2FirthStats, interceptAndOffsetFirthStats)
    )
  )

  gridTest("Run multiple regressions")(TEST_ROWS_AND_GOLDEN_STATS) { testCase =>
    testCase.goldenStats.zip(runTestOnRows(testCase.test, testCase.testRows)).foreach {
      case (g, our) =>
        compareLogitTestResults(g, our)
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
