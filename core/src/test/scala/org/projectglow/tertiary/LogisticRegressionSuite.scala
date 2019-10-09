package org.projectglow.tertiary

import breeze.linalg.DenseVector
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{expr, monotonically_increasing_id}

import org.projectglow.sql.HLSBaseTest
import org.projectglow.sql.expressions.{LikelihoodRatioTestStats, LogisticRegressionGwas, NewtonResult}
import org.projectglow.tertiary.RegressionTestUtils._
import org.projectglow.sql.HLSBaseTest
import org.projectglow.sql.expressions.{LikelihoodRatioTestStats, LogisticRegressionGwas, NewtonResult}

class LogisticRegressionSuite extends HLSBaseTest {

  private lazy val sess = spark

  private def runLRT(testData: TestData, onSpark: Boolean): Seq[LikelihoodRatioTestStats] = {
    if (onSpark) {
      import sess.implicits._

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
          expr("logistic_regression_gwas(genotypes, phenotypes, covariates, 'LRT')"))
        .orderBy("id")
        .selectExpr("expand_struct(logit)")
        .as[LikelihoodRatioTestStats]
        .collect()
        .toSeq
    } else {
      val encoder = Encoders
        .product[LikelihoodRatioTestStats]
        .asInstanceOf[ExpressionEncoder[LikelihoodRatioTestStats]]
        .resolveAndBind()
      val covariatesMatrix = twoDArrayToSparkMatrix(testData.covariates.toArray)
      val nullFit =
        LogisticRegressionGwas.fitNullModel(testData.phenotypes.toArray, covariatesMatrix)

      testData.genotypes.map { g =>
        val internalRow = LogisticRegressionGwas
          .logisticRegressionGwas(
            g.toArray,
            testData.phenotypes.toArray,
            covariatesMatrix,
            nullFit,
            LogisticRegressionGwas.logitTests("LRT"))
        encoder.fromRow(internalRow)
      }
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
    LikelihoodRatioTestStats(-0.611263, 0.54266503, Seq(2.901759e-01, 1.014851), 0.04693173)

  private val interceptOnlyV1 = TestData(
    Seq(Seq(0, 1, 2, 0, 0, 1)),
    Seq(0, 0, 1, 1, 1, 1),
    Seq(Array(1), Array(1), Array(1), Array(1), Array(1), Array(1)))
  private val interceptOnlyV1Stats =
    LikelihoodRatioTestStats(0.4768, 1.610951, Seq(0.1403952, 18.48469), 0.6935)

  private val interceptOnlyV2 = interceptOnlyV1.copy(phenotypes = Seq(0, 0, 1, 0, 1, 1))
  private val interceptOnlyV2Stats =
    LikelihoodRatioTestStats(1.4094, 4.0936366, Seq(0.26608762, 62.978730), 0.2549)

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
    LikelihoodRatioTestStats(3.1776, 23.9884595, Seq(0.007623126, 75486.900886), 0.35)

  // https://en.wikipedia.org/wiki/Separation_(statistics)
  private val completeSeparation = TestData(
    Seq(Seq(0, 1, 2, 0, 0, 1)),
    Seq(0, 0, 1, 1, 1, 1),
    Seq(Array(0), Array(0), Array(1), Array(1), Array(1), Array(1)))

  private val linearlyDependentCovariates = TestData(
    Seq(Seq(0, 1, 2, 0, 0, 1)),
    Seq(0, 0, 1, 1, 1, 1),
    Seq(Array(0, 0), Array(1, 2), Array(1, 2), Array(1, 2), Array(1, 2), Array(1, 2)))

  case class TestDataAndGoldenStats(testData: TestData, lrtStats: LikelihoodRatioTestStats)

  private val TEST_DATA_AND_GOLDEN_STATS = Seq(
    TestDataAndGoldenStats(admitStudents, admitStudentsStats),
    TestDataAndGoldenStats(interceptOnlyV1, interceptOnlyV1Stats),
    TestDataAndGoldenStats(interceptOnlyV2, interceptOnlyV2Stats),
    TestDataAndGoldenStats(gtsAndCovariates, gtsAndCovariatesStats)
  )

  private def compareLRTStats(s1: LikelihoodRatioTestStats, s2: LikelihoodRatioTestStats): Unit = {

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
    LogisticRegressionGwas.newtonIterations(
      twoDArrayToBreezeMatrix(testData.covariates.toArray),
      DenseVector(testData.phenotypes.toArray),
      None)
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
    assertThrows[UnsupportedOperationException] {
      spark
        .createDataFrame(rows)
        .repartition(10)
        .withColumn("test", monotonically_increasing_id())
        .withColumn(
          "logit",
          expr("logistic_regression_gwas(genotypes, phenotypes, covariates, test)"))
        .selectExpr("expand_struct(logit)")
        .collect()
    }
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
        compareLRTStats(testCase.lrtStats, ourStats)
    }
  }

  test("No p-value if didn't converge") {
    val ourStats = runLRT(completeSeparation, false).head
    assert(ourStats.pValue.isNaN)
  }

  test("Check sample number matches") {
    val fewerPhenoSamples = TestData(
      Seq(Seq(0, 1, 2, 0, 0, 1)),
      Seq(0, 0, 1, 1, 1),
      Seq(Array(1), Array(1), Array(1), Array(1), Array(1), Array(1)))
    val ex = intercept[IllegalArgumentException] {
      runLRT(fewerPhenoSamples, false)
    }
    assert(ex.getMessage.toLowerCase.contains("samples do not match"))
  }

  test("Checks for non-zero covariates") {
    val fewerPhenoSamples = TestData(
      Seq(Seq(0, 1, 2, 0, 0, 1)),
      Seq(0, 0, 1, 1, 1, 1),
      Seq(Array.empty, Array.empty, Array.empty, Array.empty, Array.empty, Array.empty))

    val ex = intercept[IllegalArgumentException] {
      runLRT(fewerPhenoSamples, false)
    }
    assert(ex.getMessage.toLowerCase.contains("must have at least one column"))
  }

  test("Run multiple regressions") {
    import sess.implicits._

    val sharedCovariates = interceptOnlyV1.covariates
    val rows = interceptOnlyV1.genotypes.map { g =>
        RegressionRow(
          g.toArray,
          interceptOnlyV1.phenotypes.toArray,
          twoDArrayToSparkMatrix(sharedCovariates.toArray))
      } ++ interceptOnlyV2.genotypes.map { g =>
        RegressionRow(
          g.toArray,
          interceptOnlyV2.phenotypes.toArray,
          twoDArrayToSparkMatrix(sharedCovariates.toArray))
      }

    val ourStats = spark
      .createDataFrame(rows)
      .withColumn("id", monotonically_increasing_id())
      .repartition(10)
      .withColumn(
        "logit",
        expr("logistic_regression_gwas(genotypes, phenotypes, covariates, 'LRT')"))
      .orderBy("id")
      .selectExpr("expand_struct(logit)")
      .as[LikelihoodRatioTestStats]
      .collect()
      .toSeq

    Seq(interceptOnlyV1Stats, interceptOnlyV2Stats).zip(ourStats).foreach {
      case (golden, our) =>
        compareLRTStats(golden, our)
    }
  }
}
