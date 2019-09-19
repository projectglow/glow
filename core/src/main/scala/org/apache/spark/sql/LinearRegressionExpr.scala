package org.apache.spark.sql

import org.apache.spark.ml.linalg.MatrixUDT
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, TernaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import com.databricks.hls.tertiary.{ComputeQR, CovariateQRContext, LinearRegressionGwas}

case class LinearRegressionExpr(
    genotypes: Expression,
    phenotypes: Expression,
    covariates: Expression)
    extends TernaryExpression
    with CodegenFallback
    with ImplicitCastInputTypes {

  private val matrixUDT = new MatrixUDT()

  override def dataType: DataType =
    StructType(
      Seq(
        StructField("beta", DoubleType),
        StructField("standardError", DoubleType),
        StructField("pValue", DoubleType)))

  override def inputTypes: Seq[DataType] =
    Seq(ArrayType(DoubleType), ArrayType(DoubleType), matrixUDT)

  override def children: Seq[Expression] = Seq(genotypes, phenotypes, covariates)

  private var qrFactorization: CovariateQRContext = _
  // Save the QR factorization of the covariate matrix since it's the same for every row
  private def getCovariateQR(covariates: InternalRow): CovariateQRContext = {
    if (qrFactorization == null) {
      qrFactorization = ComputeQR.computeQR(matrixUDT.deserialize(covariates).toDense)
    }
    qrFactorization
  }

  override protected def nullSafeEval(genotypes: Any, phenotypes: Any, covariates: Any): Any = {
    val covariateQR = getCovariateQR(covariates.asInstanceOf[InternalRow])
    LinearRegressionGwas.linearRegressionGwas(
      genotypes.asInstanceOf[ArrayData],
      phenotypes.asInstanceOf[ArrayData],
      covariateQR)
  }
}
