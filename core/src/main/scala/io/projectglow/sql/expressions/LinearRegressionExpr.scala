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

import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, TernaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

case class LinearRegressionExpr(
    genotypes: Expression,
    phenotypes: Expression,
    covariates: Expression)
    extends TernaryExpression
    with CodegenFallback
    with ImplicitCastInputTypes {

  private val matrixUDT = SQLUtils.newMatrixUDT()

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
