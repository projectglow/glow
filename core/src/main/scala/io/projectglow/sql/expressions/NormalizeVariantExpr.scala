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

import io.projectglow.common.VariantSchemas.{alternateAllelesField, endField, refAlleleField, startField}
import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, QuaternaryExpression, TernaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._


object NormalizeVariantExpr {

  def doVariantNormalization(start: Any, end: Any, refAllele: Any, altAlleles: Any): InternalRow = {

    LinearRegressionGwas.linearRegressionGwas(
      genotypes.asInstanceOf[ArrayData],
      phenotypes.asInstanceOf[ArrayData],
      state.get())
  }
}


case class NormalizeVariant(start: Expression,
                            end: Expression,
                            refAllele : Expression,
                            altAlleles: Expression)
  extends QuaternaryExpression
    with ImplicitCastInputTypes {

  override def prettyName: String = "normalize_variant"

  /*    lazy val testStr = test.eval().asInstanceOf[UTF8String].toString
      lazy val matrixUDT = SQLUtils.newMatrixUDT()

      private lazy val logitTest = LogisticRegressionGwas
        .logitTests
        .getOrElse(
          test.eval().asInstanceOf[UTF8String].toString,
          throw new IllegalArgumentException(
            s"Supported tests are currently: ${LogisticRegressionGwas.logitTests.keys.mkString(", ")}")
        )
  */
  override def dataType: DataType = StructType(
    Seq(
      StructField(startField.name, startField.dataType),
      StructField(endField.name, endField.dataType),
      StructField(refAlleleField.name, refAlleleField.dataType),
      StructField(alternateAllelesField.name, alternateAllelesField.dataType)
    )
  )

  override def inputTypes: Seq[DataType] =
    Seq(IntegerType, IntegerType, StringType, ArrayType(StringType))

  override def children: Seq[Expression] = Seq(start, end, refAllele, altAlleles)

  override protected def nullSafeEval(
                                       start: Any,
                                       end: Any,
                                       refAllele: Any,
                                       altAlleles: Any): Any = {
    NormalizeVariantExpr.doVariantNormalization(start, end, refAllele, altAlleles)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      (start, end, refAllele, altAlleles) => {
        s"""
           |
         |${ev.value} = io.projectglow.sql.expressions.NormalizeVariantExpr.doVariantNormalization($start, $end, $refAllele, $altAlleles);
       """.stripMargin
      }
    )
  }
}