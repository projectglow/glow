package io.projectglow.sql.expressions

import com.github.fommil.netlib.LAPACK
import org.apache.spark.ml.linalg.DenseMatrix
import org.netlib.util.intW

import io.projectglow.common.GlowLogging

/**
 * Context that can be computed once for all variant sites for a linear regression GWAS analysis.
 * @param numRows The number of rows (samples) in the design matrix
 * @param numCovariateCols The number of covariate columns (same for every variant site)
 * @param values The output from covariate QR factorization. To minimize data copying, this array
 *               contains enough extra space to store the genetic column as well, so the actual
 *               length of the array is numRows * numCols
 * @param tau The output from the QR factorization. See http://www.netlib.org/lapack/explore-html/df/dc5/group__variants_g_ecomputational_ga3766ea903391b5cf9008132f7440ec7b.html
 * @param work The work array to be used by [[LinearRegressionGwas]]
 */
case class CovariateQRContext(
    numRows: Int,
    numCovariateCols: Int,
    values: Array[Double],
    tau: Array[Double],
    work: Array[Double]) {
  def numCols: Int = numCovariateCols + 1
}

object ComputeQR extends GlowLogging {
  def computeQR(covariateMatrix: DenseMatrix): CovariateQRContext = {
    require(
      covariateMatrix.numRows > covariateMatrix.numCols,
      s"Number of covariates must be less than number of samples")

    val lapack = LAPACK.getInstance

    // for TSQR, tau has dimension = number of columns
    // We allocate space for the genotype tau here so that we can avoid copying on the hot path
    val tau = new Array[Double](covariateMatrix.numCols + 1)

    // calculate workspace size for QR factorization
    var workspace = new Array[Double](1)
    val info = new intW(0)
    lapack.dgeqrf(
      covariateMatrix.numRows,
      covariateMatrix.numCols,
      covariateMatrix.values,
      covariateMatrix.numRows,
      tau,
      workspace,
      -1,
      info)

    require(
      info.`val` == 0,
      s"Computing covariate QR workspace size failed because argument ${-info.`val`} had an illegal value")

    val workspaceSize = workspace(0).toInt
    workspace = new Array[Double](workspaceSize)
    lapack.dgeqrf(
      covariateMatrix.numRows,
      covariateMatrix.numCols,
      covariateMatrix.values,
      covariateMatrix.numRows,
      tau,
      workspace,
      workspaceSize,
      info)
    require(
      info.`val` == 0,
      s"Computing covariate QR factorization failed because argument ${-info.`val`} had an illegal value")

    // Copy covariate matrix QR factorization into a new array that can hold an extra column
    val newElements = new Array[Double](covariateMatrix.numRows * (covariateMatrix.numCols + 1))
    System.arraycopy(covariateMatrix.values, 0, newElements, 0, covariateMatrix.values.length)

    // Determine optimal work size for algorithm in [[LinearRegressionGwas]]
    val workSize = new Array[Double](1)
    lapack.dormqr(
      "L",
      "T",
      covariateMatrix.numRows,
      1,
      tau.length,
      Array.emptyDoubleArray,
      covariateMatrix.numRows,
      Array.emptyDoubleArray,
      Array.emptyDoubleArray,
      covariateMatrix.numRows,
      workSize,
      -1,
      info
    )
    require(
      info.`val` == 0,
      s"Computing optimal workspace size failed because argument ${-info.`val`} had an illegal value")
    val work = new Array[Double](workSize(0).toInt)

    CovariateQRContext(covariateMatrix.numRows, covariateMatrix.numCols, newElements, tau, work)
  }
}
