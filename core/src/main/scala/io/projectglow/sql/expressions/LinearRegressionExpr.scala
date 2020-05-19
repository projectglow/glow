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

  def getState(covariates: Any): CovariateQRContext = {
    if (state.get() == null) {
      // Save the QR factorization of the covariate matrix since it's the same for every row
      state.set(CovariateQRContext.computeQR(matrixUDT.deserialize(covariates).toDense))
      TaskContext.get().addTaskCompletionListener[Unit](_ => state.remove())
    }
    state.get()
  }

  def doSinglePhenoLinearRegression(
      genotypes: Any,
      phenotypes: Any,
      covariates: Any): InternalRow = {
    LinearRegressionGwas.linearRegressionGwas(
      new DenseVector[Double](genotypes.asInstanceOf[ArrayData].toDoubleArray()),
      new DenseVector[Double](phenotypes.asInstanceOf[ArrayData].toDoubleArray()),
      getState(covariates)
    )
  }

  def doMultiPhenoLinearRegression(genotypes: Any, phenotypes: Any, covariates: Any): ArrayData = {
    val convertedGenotypes =
      new DenseVector[Double](genotypes.asInstanceOf[ArrayData].toDoubleArray())
    val covariateQRContext = getState(covariates)

    val results = matrixUDT.deserialize(phenotypes).toDense.colIter.map(_.toArray).map {
      phenotypeArray =>
        LinearRegressionGwas.linearRegressionGwas(
          convertedGenotypes,
          new DenseVector[Double](phenotypeArray),
          covariateQRContext
        )
    }
    ArrayData.toArrayData(results.toArray)
  }
}

case class LinearRegressionExpr(
    genotypes: Expression,
    phenotypes: Expression,
    covariates: Expression)
    extends TernaryExpression
    with ImplicitCastInputTypes {

  private val matrixUDT = SQLUtils.newMatrixUDT()

  val perPhenotypeDataType: DataType = StructType(
    Seq(
      StructField("beta", DoubleType),
      StructField("standardError", DoubleType),
      StructField("pValue", DoubleType)))

  lazy val hasMultiplePhenotypes: Boolean = phenotypes.dataType match {
    case ArrayType(DoubleType, _) => false
    case _ => true
  }

  override def dataType: DataType =
    if (hasMultiplePhenotypes) {
      ArrayType(perPhenotypeDataType)
    } else {
      perPhenotypeDataType
    }

  override def inputTypes: Seq[SQLUtils.ADT] =
    Seq(
      ArrayType(DoubleType),
      SQLUtils.createTypeCollection(ArrayType(DoubleType), matrixUDT),
      matrixUDT)

  override def children: Seq[Expression] = Seq(genotypes, phenotypes, covariates)

  override protected def nullSafeEval(genotypes: Any, phenotypes: Any, covariates: Any): Any = {
    if (hasMultiplePhenotypes) {
      LinearRegressionExpr.doMultiPhenoLinearRegression(genotypes, phenotypes, covariates)
    } else {
      LinearRegressionExpr.doSinglePhenoLinearRegression(genotypes, phenotypes, covariates)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val fn = if (hasMultiplePhenotypes) {
      "doMultiPhenoLinearRegression"
    } else {
      "doSinglePhenoLinearRegression"
    }
    nullSafeCodeGen(
      ctx,
      ev,
      (genotypes, phenotypes, covariates) => {
        s"""
         |${ev.value} = io.projectglow.sql.expressions.LinearRegressionExpr.$fn($genotypes, $phenotypes, $covariates);
       """.stripMargin
      }
    )
  }
}
