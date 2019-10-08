package org.projectglow.core.sql.expressions

import com.github.fommil.netlib.{BLAS, LAPACK}
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.util.FastMath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.netlib.util.{doubleW, intW}
import org.projectglow.common.HLSLogging
import org.projectglow.core.common.HLSLogging

case class RegressionStats(beta: Double, standardError: Double, pValue: Double)

object LinearRegressionGwas extends HLSLogging {

  /**
   * Fits a linear regression model to a single variant.
   *
   * Fitting a linear regression model involves solving a linear least squares
   * problem of the form \arg\min_x ||Ax - b||_2, where the matrix A is
   * tall-and-skinny and thus the problem is overconstrained. This problem can
   * be solved using several numerical algorithms, with the QR decomposition
   * providing a reasonable tradeoff between numerical stability and efficiency.
   *
   * In the case of a GWAS analysis, we know that the matrix A is the same for each variant site
   * except for the final column, which contains the genetic observation for each sample.
   * We can use the QR update algorithms described in http://eprints.ma.man.ac.uk/1192/1/covered/MIMS_ep2008_111.pdf
   * to update the QR factorization with the new column rather than compute the factorization
   * from scratch.
   *
   * In particular, we implement the algorithm described in section 2.5.1 and the fortran routine
   * addcols.f to handle the special (simpler!) case where we add only the final column.
   *
   * In this function we use the following notation:
   * - C: The covariate matrix
   * - g: Vector containing the genetic observations for each sample
   * - G: The full design matrix (g appended to C)
   * - QR_A: The QR factorization of the matrix A (for any A)
   * - beta: The linear regression coefficient for the genotype term
   */
  def runRegression(
      genotypes: Array[Double],
      phenotypes: Array[Double],
      covariateQRContext: CovariateQRContext): RegressionStats = {
    require(
      genotypes.length == phenotypes.length,
      "Number of samples differs between genotype and phenotype arrays")
    require(
      genotypes.length == covariateQRContext.numRows,
      "Number of samples differs between genotype array and covariate matrix")

    val lapack = LAPACK.getInstance
    val blas = BLAS.getInstance

    val out = new intW(0)
    val initialNumElements = covariateQRContext.values.length - covariateQRContext.numRows

    // Overwrite genotypes with Q_C^T * g, which is what g would look like after applying the
    // transformations to factor the covariate columns
    lapack.dormqr(
      "L",
      "T",
      genotypes.length,
      1,
      covariateQRContext.numCovariateCols,
      covariateQRContext.values,
      genotypes.length,
      covariateQRContext.tau,
      genotypes,
      genotypes.length,
      covariateQRContext.work,
      covariateQRContext.work.length,
      out
    )

    require(
      out.`val` == 0,
      s"Computing Q_C ^T * g because argument ${-out.`val`} had illegal value")

    // Zero out elements below the diagonal of Q_C^T * g with a Householder transformation
    val alpha = new doubleW(genotypes(covariateQRContext.numCovariateCols))
    val v = new Array[Double](genotypes.length - covariateQRContext.numCols)
    val lastTau = new doubleW(0)
    System.arraycopy(genotypes, covariateQRContext.numCols, v, 0, v.length)
    lapack.dlarfg(v.length + 1, alpha, v, 1, lastTau)

    // We now have the complete final column of QR_G. Copy it into the QR array.
    System.arraycopy(
      genotypes,
      0,
      covariateQRContext.values,
      initialNumElements,
      covariateQRContext.numCovariateCols)
    covariateQRContext.values(initialNumElements + covariateQRContext.numCovariateCols) =
      alpha.`val`
    System.arraycopy(
      v,
      0,
      covariateQRContext.values,
      initialNumElements + covariateQRContext.numCols,
      v.length)

    // Add the final tau
    covariateQRContext.tau(covariateQRContext.tau.length - 1) = lastTau.`val`

    // Now covariateQRContext contains QR_G

    // Get d = Q_G^T * b
    lapack.dormqr(
      "L",
      "T",
      phenotypes.length,
      1,
      covariateQRContext.tau.length,
      covariateQRContext.values,
      phenotypes.length,
      covariateQRContext.tau,
      phenotypes,
      phenotypes.length,
      covariateQRContext.work,
      covariateQRContext.work.length,
      out
    )

    require(out.`val` == 0, s"Computing d failed because argument ${-out.`val`} had illegal value")

    // Now phenotypes holds d, so we solve for x (the regression coefficients)
    lapack.dtrtrs(
      "U",
      "N",
      "N",
      covariateQRContext.numCols,
      1,
      covariateQRContext.values,
      covariateQRContext.numRows,
      phenotypes,
      phenotypes.length,
      out)

    require(
      out.`val` >= 0,
      s"Solving for beta failed because argument ${-out.`val`} had illegal value")
    require(out.`val` == 0, s"Solving for beta failed because G was singular")

    val beta = phenotypes(covariateQRContext.numCovariateCols)

    // The total error is the d^T * d for the rows greater than (n_covariates + 1)
    val totalError = blas.ddot(
      phenotypes.length - covariateQRContext.numCols,
      phenotypes,
      covariateQRContext.numCols,
      1,
      phenotypes,
      covariateQRContext.numCols,
      1
    )

    // Calculate the t statistic described in
    // https://web.stanford.edu/~hastie/ElemStatLearn/printings/ESLII_print12.pdf equation
    // 3.12, we need to invert (G^T * G)
    // Note that (G^T * G) = (R_G^T * R_G)
    val qrCopy = covariateQRContext.values.clone()
    lapack.dpotri("U", covariateQRContext.numCols, qrCopy, covariateQRContext.numRows, out)

    // Get the last term along the diagonal
    val diagonalTerm = qrCopy(
      covariateQRContext.numRows * covariateQRContext.numCovariateCols + covariateQRContext.numCovariateCols)
    val standardError =
      FastMath.sqrt(
        diagonalTerm * (totalError / (covariateQRContext.numRows - covariateQRContext.numCols)))

    // t-statistic
    val t = beta / standardError
    val tDist = new TDistribution(covariateQRContext.numRows - covariateQRContext.numCols)
    val pvalue = 2 * tDist.cumulativeProbability(-Math.abs(t))
    RegressionStats(beta, standardError, pvalue)
  }

  def linearRegressionGwas(
      genotypes: ArrayData,
      phenotypes: ArrayData,
      covariateQR: CovariateQRContext): InternalRow = {

    val regressionStats =
      runRegression(genotypes.toDoubleArray, phenotypes.toDoubleArray, covariateQR)

    InternalRow(regressionStats.beta, regressionStats.standardError, regressionStats.pValue)
  }
}
