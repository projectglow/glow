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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, NumericType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Imputes the missing values of an array using the non-missing values. Values that are NaN, null or equal to the
 * missing value parameter are not included in the aggregation, and are substituted with the specified strategy.
 * Currently, the only supported strategy is mean.
 *
 * If the missing value is not provided, the parameter defaults to -1.
 */
case class Impute(array: Expression, strategy: Expression, missingValue: Expression) extends RewriteAfterResolution {
  override def children: Seq[Expression] = Seq(array, strategy, missingValue)

  def this(array: Expression, strategy: Expression) = {
    this(array, strategy, Literal(-1))
  }

  override def rewrite: Expression = {
    if (!array.dataType.isInstanceOf[ArrayType] ||
      !array.dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[NumericType]) {
      throw new IllegalArgumentException(s"Can only impute numeric array; provided type is ${array.dataType}.")
    }

    if (!strategy.dataType.isInstanceOf[StringType] ||
      strategy.eval().asInstanceOf[UTF8String].toString != "mean") {
      throw new IllegalArgumentException("The only supported strategy is `mean`.")
    }

    val nLv = NamedLambdaVariable("numArg", array.dataType.asInstanceOf[ArrayType].elementType, true)
    val isMissing = IsNaN(nLv) || IsNull(nLv) || nLv === missingValue

    val sLv = NamedLambdaVariable("structArg", StructType.fromDDL("sum double, count long"), true)
    val sumName = Literal(UTF8String.fromString("sum"), StringType)
    val countName = Literal(UTF8String.fromString("count"), StringType)
    val getSum = GetStructField(sLv, 0, Some("sum"))
    val getCount = GetStructField(sLv, 1, Some("count"))

    // Sum and count of non-missing values
    val zeros = namedStruct(sumName, Literal(0d), countName, Literal(0l))
    // Update sum and count with array value
    val updateFn = LambdaFunction(
      If(
        isMissing,
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
    // Calculate average of non-missing values
    val finalizeFn = LambdaFunction(getSum / getCount, Seq(sLv))

    // Average non-missing values
    val avg = ArrayAggregate(
      array,
      zeros,
      updateFn,
      finalizeFn
    )

    // Replace missing values with average
    ArrayTransform(
      array,
      LambdaFunction(
        If(isMissing, avg, nLv),
        Seq(nLv)
      )
    )
  }
}
