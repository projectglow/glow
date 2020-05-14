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

import io.projectglow.sql.util.RewriteAfterResolution

import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, NumericType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Substitutes the missing values of an array using the mean of the non-missing values. Values that are NaN, null or
 * equal to the missing value parameter are not included in the aggregation, and are substituted with the mean of the
 * non-missing values. If all values are missing, they are substituted with the missing value.
 *
 * If the missing value is not provided, the parameter defaults to -1.
 */
case class MeanSubstitute(array: Expression, missingValue: Expression)
    extends RewriteAfterResolution {

  override def children: Seq[Expression] = Seq(array, missingValue)

  def this(array: Expression) = {
    this(array, Literal(-1))
  }

  // A value is considered missing if it is NaN, null or equal to the missing value parameter
  def isMissing(lv: NamedLambdaVariable): Predicate = IsNaN(lv) || IsNull(lv) || lv === missingValue

  def getNamedStruct(sumValue: Expression, countValue: Expression): Expression = {
    val sumName = Literal(UTF8String.fromString("sum"), StringType)
    val countName = Literal(UTF8String.fromString("count"), StringType)
    namedStruct(sumName, sumValue, countName, countValue)
  }

  def getNumericLambdaVariable(): NamedLambdaVariable =
    NamedLambdaVariable("numArg", array.dataType.asInstanceOf[ArrayType].elementType, true)
  def getStructLambdaVariable(): NamedLambdaVariable =
    NamedLambdaVariable("structArg", StructType.fromDDL("sum double, count long"), true)

  def getSum(lv: NamedLambdaVariable): GetStructField = GetStructField(lv, 0, Some("sum"))
  def getCount(lv: NamedLambdaVariable): GetStructField = GetStructField(lv, 1, Some("count"))

  // Update sum and count with array value
  def updateFn: LambdaFunction = {
    val nLv = getNumericLambdaVariable()
    val sLv = getStructLambdaVariable()

    LambdaFunction(
      If(
        isMissing(nLv),
        // If value is missing, do not update sum and count
        sLv,
        // If value is not missing, add to sum and increment count
        LambdaFunction(
          getNamedStruct(getSum(sLv) + nLv, getCount(sLv) + 1),
          Seq(sLv, nLv)
        )
      ),
      Seq(sLv, nLv)
    )
  }

  // Calculate mean for imputation
  def finalizeFn: LambdaFunction = {
    val sLv = getStructLambdaVariable()

    LambdaFunction(
      If(
        getCount(sLv) > 0,
        // If non-missing values were found, calculate the average
        getSum(sLv) / getCount(sLv),
        // If all values were missing, substitute with missing value
        missingValue
      ),
      Seq(sLv)
    )
  }

  def getMean: Expression = {
    // Sum and count of non-missing values
    val zeros = getNamedStruct(Literal(0d), Literal(0L))

    ArrayAggregate(
      array,
      zeros,
      updateFn,
      finalizeFn
    )
  }

  override def rewrite: Expression = {
    if (!array.dataType.isInstanceOf[ArrayType] ||
      !array.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[NumericType]) {
      throw SQLUtils.newAnalysisException(
        s"Can only perform mean substitution on numeric array; provided type is ${array.dataType}.")
    }

    if (!missingValue.dataType.isInstanceOf[NumericType]) {
      throw SQLUtils.newAnalysisException(
        s"Missing value must be of numeric type; provided type is ${missingValue.dataType}.")
    }

    // Replace missing values with the provided strategy
    val nLv = getNumericLambdaVariable()
    ArrayTransform(
      array,
      LambdaFunction(
        If(isMissing(nLv), getMean, nLv),
        Seq(nLv)
      )
    )
  }
}
