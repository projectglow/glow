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
 * Imputes the missing values of an array using the non-missing values. Values that are NaN, null or equal to the
 * missing value parameter are not included in the aggregation, and are substituted with the specified strategy.
 * Currently, the supported strategies are mean and median. If all values are missing, they are substituted with the
 * missing value.
 *
 * If the missing value is not provided, the parameter defaults to -1.
 */
case class Impute(array: Expression, strategy: Expression, missingValue: Expression)
    extends RewriteAfterResolution {
  lazy val imputationStrategies: Map[String, Expression] = Map(
    "mean" -> getMean,
    "median" -> getMedian
  )

  override def children: Seq[Expression] = Seq(array, strategy, missingValue)

  def this(array: Expression, strategy: Expression) = {
    this(array, strategy, Literal(-1))
  }

  def isMissing(lv: NamedLambdaVariable): Predicate = IsNaN(lv) || IsNull(lv) || lv === missingValue

  def getMean: Expression = {
    val nLv =
      NamedLambdaVariable("numArg", array.dataType.asInstanceOf[ArrayType].elementType, true)
    val sLv = NamedLambdaVariable("structArg", StructType.fromDDL("sum double, count long"), true)
    val sumName = Literal(UTF8String.fromString("sum"), StringType)
    val countName = Literal(UTF8String.fromString("count"), StringType)
    val getSum = GetStructField(sLv, 0, Some("sum"))
    val getCount = GetStructField(sLv, 1, Some("count"))

    // Sum and count of non-missing values
    val zeros = namedStruct(sumName, Literal(0d), countName, Literal(0L))
    // Update sum and count with array value
    val updateFn = LambdaFunction(
      If(
        isMissing(nLv),
        // If value is missing, do not update sum and count
        sLv,
        // If value is not missing, add to sum and increment count
        LambdaFunction(
          namedStruct(sumName, getSum + nLv, countName, getCount + 1),
          Seq(sLv, nLv)
        )
      ),
      Seq(sLv, nLv)
    )
    // Calculate value for imputation
    val finalizeFn = LambdaFunction(
      If(
        getCount > 0,
        // If non-missing values were found, calculate the average
        getSum / getCount,
        // If all values were missing, substitute with missing value
        missingValue
      ),
      Seq(sLv)
    )

    ArrayAggregate(
      array,
      zeros,
      updateFn,
      finalizeFn
    )
  }

  def getMedian: Expression = {
    val nLv =
      NamedLambdaVariable("numArg", array.dataType.asInstanceOf[ArrayType].elementType, true)
    val filteredArray = ArrayFilter(
      array,
      LambdaFunction(
        If(
          isMissing(nLv),
          false,
          true
        ),
        Seq(nLv)
      )
    )
    val sortedArray = ArraySort(filteredArray)
    val halfIdx = Ceil(Size(sortedArray) / 2).cast(IntegerType)
    If(
      Size(sortedArray) === 0,
      missingValue,
      If(
        Size(sortedArray) % 2 === 0,
        (ElementAt(sortedArray, halfIdx) + ElementAt(sortedArray, halfIdx + 1)) / 2,
        ElementAt(sortedArray, halfIdx)
      )
    )
  }

  override def rewrite: Expression = {
    if (!array.dataType.isInstanceOf[ArrayType] ||
      !array.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[NumericType]) {
      throw new IllegalArgumentException(
        s"Can only impute numeric array; provided type is ${array.dataType}.")
    }

    if (!strategy.dataType.isInstanceOf[StringType] ||
      !imputationStrategies.keySet.contains(strategy.eval().asInstanceOf[UTF8String].toString)) {
      throw new IllegalArgumentException(
        "Supported strategies are: " + imputationStrategies.keys.mkString(", "))
    }

    // Replace missing values with the provided strategy
    val strategyStr = strategy.eval().asInstanceOf[UTF8String].toString
    val imputedValue = imputationStrategies(strategyStr)
    val nLv =
      NamedLambdaVariable("numArg", array.dataType.asInstanceOf[ArrayType].elementType, true)
    ArrayTransform(
      array,
      LambdaFunction(
        If(isMissing(nLv), imputedValue, nLv),
        Seq(nLv)
      )
    )
  }
}
