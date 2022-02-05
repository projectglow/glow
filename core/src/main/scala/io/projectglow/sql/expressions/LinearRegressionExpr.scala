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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, TernaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

object LinearRegressionExpr {
  private val matrixUDT = SQLUtils.newMatrixUDT()
  private val state = new ThreadLocal[CovariateQRContext]

  def doLinearRegression(genotypes: Any, phenotypes: Any, covariates: Any): InternalRow = {

    if (state.get() == null) {
      // Save the QR factorization of the covariate matrix since it's the same for every row
      state.set(CovariateQRContext.computeQR(matrixUDT.deserialize(covariates).toDense))
      TaskContext.get().addTaskCompletionListener[Unit](_ => state.remove())
    }

    LinearRegressionGwas.linearRegressionGwas(
      new DenseVector[Double](genotypes.asInstanceOf[ArrayData].toDoubleArray()),
      new DenseVector[Double](phenotypes.asInstanceOf[ArrayData].toDoubleArray()),
      state.get()
    )
  }
}

case class LinearRegressionExpr(
    genotypes: Expression,
    phenotypes: Expression,
    covariates: Expression)
    extends TernaryExpression
    with ImplicitCastInputTypes {

  private val matrixUDT = SQLUtils.newMatrixUDT()

  override def first: Expression = genotypes
  override def second: Expression = phenotypes
  override def third: Expression = covariates

  override def dataType: DataType =
    StructType(
      Seq(
        StructField("beta", DoubleType),
        StructField("standardError", DoubleType),
        StructField("pValue", DoubleType)))

  override def inputTypes: Seq[DataType] =
    Seq(ArrayType(DoubleType), ArrayType(DoubleType), matrixUDT)

  override protected def nullSafeEval(genotypes: Any, phenotypes: Any, covariates: Any): Any = {
    LinearRegressionExpr.doLinearRegression(genotypes, phenotypes, covariates)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      (genotypes, phenotypes, covariates) => {
        s"""
         |${ev.value} = io.projectglow.sql.expressions.LinearRegressionExpr.doLinearRegression($genotypes, $phenotypes, $covariates);
       """.stripMargin
      }
    )
  }

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): LinearRegressionExpr =
    copy(genotypes = newFirst, phenotypes = newSecond, covariates = newThird)
}
