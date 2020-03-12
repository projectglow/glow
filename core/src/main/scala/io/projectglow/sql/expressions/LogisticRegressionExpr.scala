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

import breeze.linalg.{DenseVector, DenseMatrix => BreezeDenseMatrix}
import org.apache.spark.TaskContext
import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.SparkShim.QuaternaryExpression

class LogisticRegressionState(testStr: String) {
  val logitTest = LogisticRegressionGwas
    .logitTests(testStr) // we check before evaluation that this is a valid test

  // If the null fit can be reused for each phenotype, we put the mapping in this map
  val nullFitMap: mutable.Map[Int, logitTest.FitState] = mutable.Map.empty
  // If the fit state is just allocations because the null fit can't be reused, we put the
  // singleton here
  var fitState: logitTest.FitState = _
  val matrixUDT = SQLUtils.newMatrixUDT()

  def getFitState(phenotypes: Array[Double], covariates: Any): logitTest.FitState = {
    if (!logitTest.fitStatePerPhenotype) {
      if (fitState == null) {
        fitState = logitTest.init(phenotypes, matrixUDT.deserialize(covariates).toDense)
      }
      return fitState
    }

    val phenoHash = MurmurHash3.arrayHash(phenotypes)
    if (!nullFitMap.contains(phenoHash)) {
      nullFitMap.put(
        phenoHash,
        logitTest.init(phenotypes, matrixUDT.deserialize(covariates).toDense))
    }
    nullFitMap(phenoHash)
  }
}

object LogisticRegressionExpr {
  private lazy val state = new ThreadLocal[LogisticRegressionState]()
  def getState(test: String): LogisticRegressionState = {
    if (state.get() == null) {
      state.set(new LogisticRegressionState(test))
      TaskContext.get().addTaskCompletionListener[Unit](_ => state.remove())
    }
    state.get()
  }

  def doLogisticRegression(
      test: String,
      genotypes: Any,
      phenotypes: Any,
      covariates: Any): InternalRow = {

    val state = getState(test)
    val covariatesStruct = covariates.asInstanceOf[InternalRow]
    val covariateRows = covariatesStruct.getInt(1)
    val covariateCols = covariatesStruct.getInt(2)
    val genotypeArray = genotypes.asInstanceOf[ArrayData].toDoubleArray
    val phenotypeArray = phenotypes.asInstanceOf[ArrayData].toDoubleArray
    require(
      genotypeArray.length == phenotypeArray.length,
      "Number of samples differs between genotype and phenotype arrays")
    require(covariateCols > 0, "Covariate matrix must have at least one column")
    require(
      covariateRows == phenotypeArray.length,
      "Number of samples do not match between phenotype vector and covariate matrix"
    )

    val nullFitState = state.getFitState(phenotypeArray, covariates)
    state
      .logitTest
      .runTest(
        new DenseVector[Double](genotypeArray),
        new DenseVector[Double](phenotypeArray),
        nullFitState
      )
  }
}

case class LogisticRegressionExpr(
    genotypes: Expression,
    phenotypes: Expression,
    covariates: Expression,
    test: Expression)
    extends QuaternaryExpression
    with ImplicitCastInputTypes {

  override def prettyName: String = "logistic_regression_gwas"

  lazy val testStr = test.eval().asInstanceOf[UTF8String].toString
  lazy val matrixUDT = SQLUtils.newMatrixUDT()

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

  override protected def nullSafeEval(
      genotypes: Any,
      phenotypes: Any,
      covariates: Any,
      test: Any): Any = {
    LogisticRegressionExpr.doLogisticRegression(testStr, genotypes, phenotypes, covariates)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      (genotypes, phenotypes, covariates, _) => {
        s"""
         |
         |${ev.value} = io.projectglow.sql.expressions.LogisticRegressionExpr.doLogisticRegression("$testStr", $genotypes, $phenotypes, $covariates);
       """.stripMargin
      }
    )
  }
}
