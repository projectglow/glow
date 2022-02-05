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

import breeze.linalg.DenseVector
import org.apache.spark.TaskContext
import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, QuinaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

class LogisticRegressionState(testStr: String) {
  val logitTest = LogisticRegressionGwas
    .logitTests(testStr) // we check before evaluation that this is a valid test

  // If the null fit can be reused for each (phenotype, offset), we put the mapping in this map
  val nullFitMap: mutable.Map[(Int, Int), logitTest.FitState] = mutable.Map.empty
  // If the fit state is just allocations because the null fit can't be reused, we put the
  // singleton here
  var fitState: logitTest.FitState = _
  val matrixUDT = SQLUtils.newMatrixUDT()

  def getFitState(
      phenotypes: Array[Double],
      covariates: Any,
      offsetOption: Option[Array[Double]]): logitTest.FitState = {
    if (!logitTest.fitStatePerPhenotype) {
      if (fitState == null) {
        fitState =
          logitTest.init(phenotypes, matrixUDT.deserialize(covariates).toDense, offsetOption)
      }
      return fitState
    }

    val phenoHash = MurmurHash3.arrayHash(phenotypes)
    val offsetHash = MurmurHash3.arrayHash(offsetOption.getOrElse(Array[Double]()))
    if (!nullFitMap.contains((phenoHash, offsetHash))) {
      nullFitMap.put(
        (phenoHash, offsetHash),
        logitTest.init(phenotypes, matrixUDT.deserialize(covariates).toDense, offsetOption))
    }
    nullFitMap((phenoHash, offsetHash))
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
    doLogisticRegression(test, genotypes, phenotypes, covariates, None)
  }

  def doLogisticRegression(
      test: String,
      genotypes: Any,
      phenotypes: Any,
      covariates: Any,
      offset: Any): InternalRow = {
    doLogisticRegression(test, genotypes, phenotypes, covariates, Some(offset))
  }

  def doLogisticRegression(
      test: String,
      genotypes: Any,
      phenotypes: Any,
      covariates: Any,
      offsetOption: Option[Any]): InternalRow = {

    val state = getState(test)
    val covariatesStruct = covariates.asInstanceOf[InternalRow]
    val covariateRows = covariatesStruct.getInt(1)
    val covariateCols = covariatesStruct.getInt(2)
    val genotypeArray = genotypes.asInstanceOf[ArrayData].toDoubleArray
    val phenotypeArray = phenotypes.asInstanceOf[ArrayData].toDoubleArray
    val offsetArrayOption = offsetOption.map(_.asInstanceOf[ArrayData].toDoubleArray)
    require(
      genotypeArray.length == phenotypeArray.length,
      "Number of samples differs between genotype and phenotype arrays")
    require(covariateCols > 0, "Covariate matrix must have at least one column")
    require(
      covariateRows == phenotypeArray.length,
      "Number of samples do not match between phenotype vector and covariate matrix"
    )
    if (offsetArrayOption.isDefined) {
      require(
        offsetArrayOption.get.length == phenotypeArray.length,
        "Number of samples do not match between phenotype vector and offset vector"
      )
    }

    val nullFitState = state.getFitState(phenotypeArray, covariates, offsetArrayOption)
    state
      .logitTest
      .runTest(
        new DenseVector[Double](genotypeArray),
        new DenseVector[Double](phenotypeArray),
        offsetArrayOption.map(new DenseVector[Double](_)),
        nullFitState
      )
  }
}

case class LogisticRegressionExpr(
    genotypes: Expression,
    phenotypes: Expression,
    covariates: Expression,
    test: Expression,
    offsetOption: Option[Expression])
    extends QuinaryExpression
    with ImplicitCastInputTypes {

  def this(
      genotypes: Expression,
      phenotypes: Expression,
      covariates: Expression,
      test: Expression) = {
    this(genotypes, phenotypes, covariates, test, None)
  }

  def this(
      genotypes: Expression,
      phenotypes: Expression,
      covariates: Expression,
      test: Expression,
      offset: Expression) = {
    this(genotypes, phenotypes, covariates, test, Some(offset))
  }

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

  override def inputTypes: Seq[DataType] = {
    Seq(ArrayType(DoubleType), ArrayType(DoubleType), matrixUDT, StringType) ++ offsetOption.map(
      _ => ArrayType(DoubleType))
  }

  override def children: Seq[Expression] =
    Seq(genotypes, phenotypes, covariates, test) ++ offsetOption

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
      test: Any,
      offset: Option[Any]): Any = {
    LogisticRegressionExpr.doLogisticRegression(testStr, genotypes, phenotypes, covariates, offset)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      (genotypes, phenotypes, covariates, _, offset) => {
        val offsetString = offset.map(s => s", $s").getOrElse("")
        s"""
             |
             |${ev.value} = io.projectglow.sql.expressions.LogisticRegressionExpr.doLogisticRegression("$testStr", $genotypes, $phenotypes, $covariates$offsetString);
       """.stripMargin
      }
    )
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): LogisticRegressionExpr = {
    if (newChildren.size == 5) {
      copy(
        genotypes = newChildren(0),
        phenotypes = newChildren(1),
        covariates = newChildren(2),
        test = newChildren(3),
        offsetOption = Option(newChildren(4))
      )
    } else {
      copy(
        genotypes = newChildren(0),
        phenotypes = newChildren(1),
        covariates = newChildren(2),
        test = newChildren(3)
      )
    }
  }
}
