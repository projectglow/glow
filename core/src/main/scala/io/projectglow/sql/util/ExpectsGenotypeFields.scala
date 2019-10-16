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

package io.projectglow.sql.util

import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

/**
 * A trait to simplify type checking and reading for expressions that operate on arrays of genotype
 * data with the expectation that certain fields exists.
 */
trait ExpectsGenotypeFields extends Expression {
  protected def genotypesExpr: Expression

  protected def genotypeFieldsRequired: Seq[StructField]

  protected def optionalGenotypeFields: Seq[StructField] = Seq.empty

  private lazy val gStruct = genotypesExpr
    .dataType
    .asInstanceOf[ArrayType]
    .elementType
    .asInstanceOf[StructType]

  protected lazy val genotypeFieldIndices: Seq[Int] = {
    genotypeFieldsRequired.map { f =>
      gStruct.indexWhere(SQLUtils.structFieldsEqualExceptNullability(f, _))
    }
  }

  protected lazy val optionalFieldIndices: Seq[Int] = {
    optionalGenotypeFields.map { f =>
      gStruct.indexWhere(SQLUtils.structFieldsEqualExceptNullability(f, _))
    }
  }

  protected lazy val genotypeStructSize: Int = {
    gStruct.length
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    genotypesExpr.dataType match {
      case ArrayType(StructType(_), _) => // Expected
      case _ =>
        return TypeCheckResult.TypeCheckFailure("Genotypes field must be an array of structs")
    }

    val missingFields = genotypeFieldsRequired.zip(genotypeFieldIndices).collect {
      case (f, -1) =>
        f
    }

    if (missingFields.isEmpty) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      val fieldStrs = missingFields.map(f => s"name: ${f.name}, type: ${f.dataType}")
      val msg = s"Genotype struct was missing required fields: ${fieldStrs.mkString(", ")}"
      TypeCheckResult.TypeCheckFailure(msg)
    }
  }

}
