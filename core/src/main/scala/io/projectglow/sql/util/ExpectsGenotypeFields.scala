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
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedException}
import org.apache.spark.sql.catalyst.expressions.{Expression, Unevaluable}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}

/**
 * Stores the indices of required and optional fields within the genotype element struct after
 * resolution.
 * @param size The number of fields in the struct
 * @param requiredFieldIndices The indices of required fields. 0 <= idx < size.
 * @param optionalFieldIndices The indices of optional fields. -1 if not the field is not present.
 */
case class GenotypeInfo(size: Int, requiredFieldIndices: Seq[Int], optionalFieldIndices: Seq[Int])

/**
 * A trait to simplify type checking and reading for expressions that operate on arrays of genotype
 * data with the expectation that certain fields exists.
 *
 * Note: This trait introduces complexity during resolution and analysis, and prevents
 * nested column pruning. Prefer writing new functions as rewrites when possible.
 */
@deprecated("Write functions as rewrites when possible", "0.4.1")
trait ExpectsGenotypeFields extends Expression {
  def genotypeInfo: Option[GenotypeInfo]
  final def getGenotypeInfo: GenotypeInfo = {
    genotypeInfo.get
  }

  override lazy val resolved: Boolean = {
    childrenResolved && genotypeInfo.isDefined
  }

  /**
   * Make a copy of this expression with [[GenotypeInfo]] filled in.
   */
  protected def withGenotypeInfo(genotypeInfo: GenotypeInfo): Expression

  /**
   * Resolve the required field names into positions within the [[genotypesExpr]] element struct.
   *
   * This function should only be called after a successful type check.
   *
   * @return A new expression with a defined [[GenotypeInfo]]
   */
  def resolveGenotypeInfo(): Expression = {
    val info = GenotypeInfo(genotypeStructSize, requiredFieldIndices, optionalFieldIndices)
    withGenotypeInfo(info)
  }

  protected def genotypesExpr: Expression

  protected def requiredGenotypeFields: Seq[StructField]

  protected def optionalGenotypeFields: Seq[StructField] = Seq.empty

  private def gStruct =
    genotypesExpr
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]

  private def requiredFieldIndices: Seq[Int] = {
    requiredGenotypeFields.map { f =>
      gStruct.indexWhere(SQLUtils.structFieldsEqualExceptNullability(f, _))
    }
  }

  private def optionalFieldIndices: Seq[Int] = {
    optionalGenotypeFields.map { f =>
      gStruct.indexWhere(SQLUtils.structFieldsEqualExceptNullability(f, _))
    }
  }

  private def genotypeStructSize: Int = {
    gStruct.length
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    genotypesExpr.dataType match {
      case ArrayType(StructType(_), _) => // Expected
      case _ =>
        return TypeCheckResult.TypeCheckFailure("Genotypes field must be an array of structs")
    }

    val missingFields = requiredGenotypeFields.zip(requiredFieldIndices).collect {
      case (f, -1) =>
        f
    }

    if (missingFields.isEmpty) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      val fieldStrs = missingFields.map(f => s"(name: ${f.name}, type: ${f.dataType})")
      val msg = s"Genotype struct was missing required fields: ${fieldStrs.mkString(", ")}"
      TypeCheckResult.TypeCheckFailure(msg)
    }
  }
}

/**
 * Expressions that should be rewritten eagerly. The rewrite must be able to be performed without
 * knowing the datatype or nullability of any of the children.
 *
 * In general, rewrite expressions should extend this trait unless they have a compelling reason
 * to inspect their children.
 */
trait Rewrite extends Expression with Unevaluable {
  def rewrite: Expression

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
}

/**
 * Rewrites that depend on child expressions.
 */
trait RewriteAfterResolution extends Expression with Unevaluable {
  def rewrite: Expression

  override def dataType: DataType = rewrite.dataType
  override def nullable: Boolean = rewrite.nullable
}
