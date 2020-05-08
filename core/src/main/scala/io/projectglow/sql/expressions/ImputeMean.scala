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
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, LongType, StringType, StructType}

/**
 * Imputes the missing values of an array using the mean of the non-missing values. Values that are NaN, null or
 * equal to the missing value parameter are not included in the aggregation, and are substituted with the mean.
 *
 * If the missing value is not provided, the parameter defaults to -1.
 */
case class ImputeMean(array: Expression, missingValue: Expression) extends RewriteAfterResolution {
  override def children: Seq[Expression] = Seq(array, missingValue)

  def this(array: Expression) = {
    this(array, Literal(-1))
  }

  override def rewrite: Expression = {
    val nLv = NamedLambdaVariable(
      "numArg",
      array.dataType.asInstanceOf[ArrayType].elementType,
      true
    )
    val sLv = NamedLambdaVariable("structArg", StructType.fromDDL("sum double, count long"), true)

    // Average non-missing values
    val avg = ArrayAggregate(
      array,
      // Sum and count of non-missing values
      CreateNamedStruct(
        Seq(
          Literal("sum", StringType),
          Literal(0d, DoubleType),
          Literal("count", StringType),
          Literal(0L, LongType)
        )
      ),
      LambdaFunction(
        If(
          Or(IsNaN(nLv), Or(IsNull(nLv), EqualTo(nLv, missingValue))),
          sLv,
          // If value is not missing, add to sum and increment count
          LambdaFunction(
            CreateNamedStruct(
              Seq(
                Literal("sum", StringType),
                Add(
                  GetStructField(sLv, 0, Some("sum")),
                  nLv
                ),
                Literal("count", StringType),
                Add(
                  GetStructField(sLv, 1, Some("count")),
                  Literal(1L, LongType)
                )
              )
            ),
            Seq(sLv, nLv)
          )
        ),
        Seq(sLv, nLv)
      ),
      // Calculate average of non-missing values
      LambdaFunction(
        Divide(
          GetStructField(sLv, 0, Some("sum")),
          GetStructField(sLv, 1, Some("count"))
        ),
        Seq(sLv)
      )
    )

    // Replace missing values with average
    ArrayTransform(
      array,
      LambdaFunction(
        If(
          Or(IsNaN(nLv), Or(IsNull(nLv), EqualTo(nLv, missingValue))),
          avg,
          nLv
        ),
        Seq(nLv)
      )
    )
  }
}
