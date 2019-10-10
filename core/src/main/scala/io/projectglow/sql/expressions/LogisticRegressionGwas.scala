package io.projectglow.sql.expressions

import breeze.linalg._
import breeze.numerics._
import com.google.common.annotations.VisibleForTesting
import org.apache.commons.math3.distribution.{ChiSquaredDistribution, NormalDistribution}
import org.apache.spark.ml.linalg.{DenseMatrix => SparkDenseMatrix}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.StructType

import io.projectglow.common.GlowLogging

/**
 * Statistics returned upon performing a likelihood ratio test.
 *
 * @param beta Log-odds associated with the genotype, NaN iff test does not require full model fit
 * @param oddsRatio Odds ratio associated with the genotype
 * @param waldConfidenceInterval Wald 95% confidence interval of the odds ratio, NaN iff Newton iterations exploded
 * @param pValue P-value for the specified test, NaN iff the fit failed
 */
case class LikelihoodRatioTestStats(
    beta: Double,
    oddsRatio: Double,
    waldConfidenceInterval: Seq[Double],
    pValue: Double)

/**
 * Some of the logic used for logistic regression is from the Hail project.
 * The Hail project can be found on Github: https://github.com/hail-is/hail.
 * The Hail project is under an MIT license: https://github.com/hail-is/hail/blob/master/LICENSE.
 */
object LogisticRegressionGwas extends GlowLogging {
  val logitTests: Map[String, LogitTest] = Map("LRT" -> LikelihoodRatioTest)
  val zScore: Double = new NormalDistribution().inverseCumulativeProbability(.975) // Two-sided 95% confidence

  @VisibleForTesting
  private[projectglow] def newtonIterations(
      X: DenseMatrix[Double],
      y: DenseVector[Double],
      nullFitOpt: Option[NewtonArguments],
      maxIter: Int = 25,
      tolerance: Double = 1e-6): NewtonResult = {

    val args = new NewtonArguments(X, y, nullFitOpt)

    var iter = 1
    var converged = false
    var exploded = false

    val deltaB = DenseVector.zeros[Double](X.cols)

    while (!converged && !exploded && iter <= maxIter) {
      try {
        deltaB := args.fisher \ args.score // Solve for Newton-Raphson step

        if (deltaB(0).isNaN) {
          exploded = true
        } else if (max(abs(deltaB)) < tolerance) {
          converged = true
        } else {
          iter += 1
          args.b += deltaB // Parameter update
          args.mu := sigmoid(X * args.b) // Fitted probability
          args.score := X.t * (y - args.mu) // Gradient
          args.fisher := X.t * (X(::, *) *:* (args.mu *:* (1d - args.mu))) // Hessian
        }
      } catch {
        case _: breeze.linalg.MatrixSingularException => exploded = true
        case _: breeze.linalg.NotConvergedException => exploded = true
      }
    }

    val logLkhd = sum(breeze.numerics.log((y *:* args.mu) + ((1d - y) *:* (1d - args.mu))))

    NewtonResult(args, logLkhd, iter, converged, exploded)
  }

  def logisticRegressionGwas(
      genotypes: Array[Double],
      phenotypes: Array[Double],
      covariates: SparkDenseMatrix,
      nullFit: NewtonResult,
      logitTest: LogitTest): InternalRow = {
    val fullX =
      new DenseMatrix(covariates.numRows, covariates.numCols + 1, covariates.values ++ genotypes)
    val y = new DenseVector(phenotypes)

    logitTest match {
      case nullFitTest: LogitTestWithNullModelFit => nullFitTest.runTest(nullFit)
      case nullAndFullFitTest: LogitTestWithNullAndFullModelFit =>
        val fullFit = newtonIterations(fullX, y, Some(nullFit.args))
        nullAndFullFitTest.runTest(nullFit, fullFit)
    }
  }

  def fitNullModel(phenotypes: Array[Double], covariates: SparkDenseMatrix): NewtonResult = {
    val nullX = new DenseMatrix(covariates.numRows, covariates.numCols, covariates.values)
    val y = new DenseVector(phenotypes)
    newtonIterations(nullX, y, None)
  }
}

class NewtonArguments(
    X: DenseMatrix[Double],
    y: DenseVector[Double],
    nullFitArgsOpt: Option[NewtonArguments]) {

  require(
    y.length == X.rows,
    "Number of samples do not match between phenotype vector and covariate matrix")

  val n: Int = X.rows
  val m: Int = X.cols
  val b: DenseVector[Double] = DenseVector.zeros[Double](m)
  val mu: DenseVector[Double] = DenseVector.zeros[Double](n)
  val score: DenseVector[Double] = DenseVector.zeros[Double](m)
  val fisher: DenseMatrix[Double] = DenseMatrix.zeros[Double](m, m)

  if (nullFitArgsOpt.isEmpty) {
    require(X.cols > 0, "Covariate matrix must have at least one column")

    val avg = sum(y) / n
    b(0) = math.log(avg / (1 - avg))
    mu := sigmoid(X * b)
    score := X.t * (y - mu)
    fisher := X.t * (X(::, *) *:* (mu *:* (1d - mu)))
  } else {
    // Warm-start from null model fit
    val nullFitArgs = nullFitArgsOpt.get
    val m0 = nullFitArgs.b.length

    val r0 = 0 until m0
    val r1 = m0 to -1

    val X0 = X(::, r0)
    val X1 = X(::, r1)

    b(r0) := nullFitArgs.b
    mu := sigmoid(X * b)
    score(r0) := nullFitArgs.score
    score(r1) := X1.t * (y - mu)
    fisher(r0, r0) := nullFitArgs.fisher
    fisher(r0, r1) := X0.t * (X1(::, *) *:* (mu *:* (1d - mu)))
    fisher(r1, r0) := fisher(r0, r1).t
    fisher(r1, r1) := X1.t * (X1(::, *) *:* (mu *:* (1d - mu)))
  }
}

case class NewtonResult(
    args: NewtonArguments,
    logLkhd: Double,
    nIter: Int,
    converged: Boolean,
    exploded: Boolean)

trait LogitTest extends Serializable {
  def resultSchema: StructType
}

trait LogitTestWithNullModelFit extends LogitTest {
  def runTest(nullFit: NewtonResult): InternalRow
}

trait LogitTestWithNullAndFullModelFit extends LogitTest {
  def runTest(nullFit: NewtonResult, fullFit: NewtonResult): InternalRow
}

object LikelihoodRatioTest extends LogitTestWithNullAndFullModelFit {
  override def resultSchema: StructType = Encoders.product[LikelihoodRatioTestStats].schema
  override def runTest(nullFit: NewtonResult, fullFit: NewtonResult): InternalRow = {
    val beta = fullFit.args.b(-1)
    val oddsRatio = math.exp(beta)

    val waldConfidenceInterval = if (!fullFit.exploded) {
      val covarianceMatrix = inv(fullFit.args.fisher)
      val variance = diag(covarianceMatrix)
      val standardError = math.sqrt(variance(-1))
      val halfWidth = LogisticRegressionGwas.zScore * standardError

      val beta = fullFit.args.b(-1)
      Array(beta - halfWidth, beta + halfWidth).map(math.exp)
    } else {
      Array(Double.NaN, Double.NaN)
    }

    val pValue = if (fullFit.converged) {
      val chi2 = 2 * (fullFit.logLkhd - nullFit.logLkhd)
      val df = fullFit.args.m - nullFit.args.m
      val chi2Dist = new ChiSquaredDistribution(df)
      1 - chi2Dist.cumulativeProbability(Math.abs(chi2)) // 1-sided p-value
    } else {
      Double.NaN
    }

    InternalRow(beta, oddsRatio, ArrayData.toArrayData(waldConfidenceInterval), pValue)
  }
}
