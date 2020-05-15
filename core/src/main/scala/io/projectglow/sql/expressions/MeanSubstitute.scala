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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, NumericType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.sql.dsl._
import io.projectglow.sql.util.RewriteAfterResolution

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

  private lazy val arrayElementType = array.dataType.asInstanceOf[ArrayType].elementType

  // A value is considered missing if it is NaN, null or equal to the missing value parameter
  def isMissing(arrayElement: Expression): Predicate =
    IsNaN(arrayElement) || IsNull(arrayElement) || arrayElement === missingValue

  def createNamedStruct(sumValue: Expression, countValue: Expression): Expression = {
    val sumName = Literal(UTF8String.fromString("sum"), StringType)
    val countName = Literal(UTF8String.fromString("count"), StringType)
    namedStruct(sumName, sumValue, countName, countValue)
  }

  // Update sum and count with array element if not missing
  def updateSumAndCountConditionally(
      stateStruct: Expression,
      arrayElement: Expression): Expression = {
    If(
      isMissing(arrayElement),
      // If value is missing, do not update sum and count
      stateStruct,
      // If value is not missing, add to sum and increment count
      createNamedStruct(
        stateStruct.getField("sum") + arrayElement,
        stateStruct.getField("count") + 1)
    )
  }

  // Calculate mean for imputation
  def calculateMean(stateStruct: Expression): Expression = {
    If(
      stateStruct.getField("count") > 0,
      // If non-missing values were found, calculate the average
      stateStruct.getField("sum") / stateStruct.getField("count"),
      // If all values were missing, substitute with missing value
      missingValue
    )
  }

  lazy val arrayMean: Expression = {
    // Sum and count of non-missing values
    array.aggregate(
      createNamedStruct(Literal(0d), Literal(0L)),
      updateSumAndCountConditionally,
      calculateMean
    )
  }

  def substituteWithMean(arrayElement: Expression): Expression = {
    If(isMissing(arrayElement), arrayMean, arrayElement)
  }

  override def rewrite: Expression = {
    if (!array.dataType.isInstanceOf[ArrayType] || !arrayElementType.isInstanceOf[NumericType]) {
      throw SQLUtils.newAnalysisException(
        s"Can only perform mean substitution on numeric array; provided type is ${array.dataType}.")
    }

    if (!missingValue.dataType.isInstanceOf[NumericType]) {
      throw SQLUtils.newAnalysisException(
        s"Missing value must be of numeric type; provided type is ${missingValue.dataType}.")
    }

    // Replace missing values with the provided strategy
    array.arrayTransform(substituteWithMean(_))
  }
}
