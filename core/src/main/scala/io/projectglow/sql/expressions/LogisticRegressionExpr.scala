package io.projectglow.sql.expressions

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, QuaternaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class LogisticRegressionExpr(
    genotypes: Expression,
    phenotypes: Expression,
    covariates: Expression,
    test: Expression)
    extends QuaternaryExpression
    with CodegenFallback
    with ImplicitCastInputTypes {

  private val matrixUDT = SQLUtils.newMatrixUDT()

  private val logitTest = LogisticRegressionGwas
    .logitTests
    .getOrElse(
      test.eval().asInstanceOf[UTF8String].toString,
      throw new IllegalArgumentException("Supported tests are currently: LRT"))

  override def dataType: DataType = logitTest.resultSchema

  override def inputTypes: Seq[DataType] =
    Seq(ArrayType(DoubleType), ArrayType(DoubleType), matrixUDT, StringType)

  override def children: Seq[Expression] = Seq(genotypes, phenotypes, covariates, test)

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes()
    if (!test.foldable) {
      TypeCheckResult.TypeCheckFailure("Test must be a constant value")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  private val nullFitMap: mutable.Map[Int, NewtonResult] = mutable.Map.empty
  // For each phenotype, save the null model fit of the covariate matrix since it's the same for every genotype
  private def fitNullModel(phenotypes: Array[Double], covariates: DenseMatrix): NewtonResult = {
    val phenoHash = MurmurHash3.arrayHash(phenotypes)
    if (!nullFitMap.contains(phenoHash)) {
      nullFitMap.put(phenoHash, LogisticRegressionGwas.fitNullModel(phenotypes, covariates))
    }
    nullFitMap(phenoHash)
  }

  override protected def nullSafeEval(
      genotypes: Any,
      phenotypes: Any,
      covariates: Any,
      test: Any): Any = {

    val genotypeArray = genotypes.asInstanceOf[ArrayData].toDoubleArray
    val phenotypeArray = phenotypes.asInstanceOf[ArrayData].toDoubleArray
    val covariateMatrix = matrixUDT.deserialize(covariates.asInstanceOf[InternalRow]).toDense
    val testName = test.asInstanceOf[UTF8String].toString

    val nullFitNewtonResult = fitNullModel(phenotypeArray, covariateMatrix)
    LogisticRegressionGwas.logisticRegressionGwas(
      genotypeArray,
      phenotypeArray,
      covariateMatrix,
      nullFitNewtonResult,
      logitTest)
  }
}
