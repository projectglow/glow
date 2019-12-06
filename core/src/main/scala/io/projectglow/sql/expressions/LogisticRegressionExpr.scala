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

package io.projectglow.sql.expressions

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

import breeze.linalg.{DenseMatrix => BreezeDenseMatrix, DenseVector}
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

  override def prettyName: String = "logistic_regression_gwas"

  private val matrixUDT = SQLUtils.newMatrixUDT()

  private lazy val logitTest = LogisticRegressionGwas
    .logitTests
    .getOrElse(
      test.eval().asInstanceOf[UTF8String].toString,
      throw new IllegalArgumentException(
        s"Supported tests are currently: ${LogisticRegressionGwas.logitTests.keys.mkString(", ")}")
    )

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

  // For each phenotype, save the null model fit of the covariate matrix since it's the same for every genotype
  private val nullFitMap: mutable.Map[Int, logitTest.FitState] = mutable.Map.empty
  private var fitState: logitTest.FitState = _

  private def getFitState(
      phenotypes: Array[Double],
      covariates: DenseMatrix): logitTest.FitState = {
    if (!logitTest.canReuseNullFit) {
      if (fitState == null) {
        fitState = logitTest.init(phenotypes, covariates)
      }
      return fitState
    }

    val phenoHash = MurmurHash3.arrayHash(phenotypes)
    if (!nullFitMap.contains(phenoHash)) {
      nullFitMap.put(phenoHash, logitTest.init(phenotypes, covariates))
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
    require(
      genotypeArray.length == phenotypeArray.length,
      "Number of samples differs between genotype and phenotype arrays")
    val covariateMatrix = matrixUDT.deserialize(covariates.asInstanceOf[InternalRow]).toDense
    require(covariateMatrix.numCols > 0, "Covariate matrix must have at least one column")
    require(
      covariateMatrix.numRows == phenotypeArray.length,
      "Number of samples do not match between phenotype vector and covariate matrix"
    )

    val nullFitNewtonResult = getFitState(phenotypeArray, covariateMatrix)
    logitTest.runTest(
      new DenseVector[Double](genotypeArray),
      new DenseVector[Double](phenotypeArray),
      nullFitNewtonResult
    )
  }
}
