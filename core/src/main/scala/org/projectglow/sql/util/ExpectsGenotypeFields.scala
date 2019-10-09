package org.projectglow.sql.util

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

  protected lazy val genotypeFieldIndices: Seq[Int] = {
    val gStruct = genotypesExpr
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]
    genotypeFieldsRequired.map { f =>
      gStruct.indexWhere(SQLUtils.structFieldsEqualExceptNullability(f, _))
    }
  }

  protected lazy val genotypeStructSize: Int = {
    val gStruct = genotypesExpr
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]
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
