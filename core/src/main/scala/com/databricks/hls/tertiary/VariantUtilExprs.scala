package com.databricks.hls.tertiary

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StructField}
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.vcf.VariantSchemas

/**
 * Implementations of utility functions for transforming variant representations. These
 * implementations are called during both whole-stage codegen and interpreted execution.
 *
 * The functions are exposed to the user as Catalyst expressions.
 */
object VariantUtilExprs {
  def genotypeStates(genotypes: ArrayData, genotypesSize: Int, genotypeIdx: Int): ArrayData = {
    val output = new Array[Int](genotypes.numElements())
    var i = 0
    while (i < output.length) {
      val calls = genotypes
        .getStruct(i, genotypesSize)
        .getStruct(genotypeIdx, 2)
        .getArray(0)
      var sum = 0
      var j = 0
      while (j < calls.numElements() && sum >= 0) {
        if (calls.getInt(j) >= 0) {
          sum += calls.getInt(j)
        } else {
          sum = -1 // missing
        }
        j += 1
      }
      output(i) = if (j == 0) -1 else sum
      i += 1
    }

    new GenericArrayData(output)
  }

  def isSnp(refAllele: UTF8String, altAllele: UTF8String): Boolean = {
    if (refAllele.numChars() != altAllele.numChars()) {
      return false
    }

    var i = 0
    var nMismatches = 0
    val refBytes = refAllele.getBytes
    val altBytes = altAllele.getBytes
    while (i < refBytes.length) {
      if (refBytes(i) != altBytes(i)) {
        nMismatches += 1
      }
      i += 1
    }

    nMismatches == 1
  }

  def isTransition(refAllele: UTF8String, altAllele: UTF8String): Boolean = {
    var i = 0
    val refBytes = refAllele.getBytes
    val altBytes = altAllele.getBytes
    while (i < refBytes.length) {
      val transition = (refBytes(i) == 'A' && altBytes(i) == 'G') ||
        (refBytes(i) == 'G' && altBytes(i) == 'A') ||
        (refBytes(i) == 'C' && altBytes(i) == 'T') ||
        (refBytes(i) == 'T' && altBytes(i) == 'C')
      if (transition) {
        return true
      }

      i += 1
    }

    false
  }

  def isTransversion(refAllele: UTF8String, altAllele: UTF8String): Boolean = {
    var i = 0
    val refBytes = refAllele.getBytes
    val altBytes = altAllele.getBytes
    while (i < refBytes.length) {
      val transversion = (refBytes(i) == 'A' && altBytes(i) == 'C') ||
        (refBytes(i) == 'A' && altBytes(i) == 'T') ||
        (refBytes(i) == 'C' && altBytes(i) == 'A') ||
        (refBytes(i) == 'C' && altBytes(i) == 'G') ||
        (refBytes(i) == 'G' && altBytes(i) == 'C') ||
        (refBytes(i) == 'G' && altBytes(i) == 'T') ||
        (refBytes(i) == 'T' && altBytes(i) == 'A') ||
        (refBytes(i) == 'T' && altBytes(i) == 'G')
      if (transversion) {
        return true
      }

      i += 1
    }

    false
  }

  def isInsertion(refAllele: UTF8String, altAllele: UTF8String): Boolean = {
    refAllele.numChars() < altAllele.numChars() && altAllele.numChars() >= 2 &&
      refAllele.substring(0, 1) == altAllele.substring(0, 1) &&
      altAllele.endsWith(refAllele.substring(1, refAllele.numChars()))
  }

  def isDeletion(refAllele: UTF8String, altAllele: UTF8String): Boolean = {
    refAllele.numChars() > altAllele.numChars() && refAllele.numChars() >= 2 &&
      refAllele.substring(0, 1) == altAllele.substring(0, 1) &&
      refAllele.endsWith(altAllele.substring(1, altAllele.numChars()))
  }

  private val star = UTF8String.fromString("*")
  def variantType(refAllele: UTF8String, altAllele: UTF8String): VariantType = {
    if (isSnp(refAllele, altAllele) && isTransition(refAllele, altAllele)) {
      VariantType.Transition
    } else if (isSnp(refAllele, altAllele) && isTransversion(refAllele, altAllele)) {
      VariantType.Transversion
    } else if (altAllele == star) {
      VariantType.SpanningDeletion
    } else if (isInsertion(refAllele, altAllele)) {
      VariantType.Insertion
    } else if (isDeletion(refAllele, altAllele)) {
      VariantType.Deletion
    } else {
      VariantType.Unknown
    }
  }
}

trait VariantType
object VariantType {
  case object Insertion extends VariantType
  case object Deletion extends VariantType
  case object Transition extends VariantType
  case object Transversion extends VariantType
  case object SpanningDeletion extends VariantType
  case object Unknown extends VariantType
}

/**
 * Converts a complex genotype array into an array of ints, where each element is the sum
 * of the calls array for the sample at that position if no calls are missing, or -1 if any calls
 * are missing.
 */
case class GenotypeStates(genotypes: Expression)
  extends UnaryExpression
  with ExpectsGenotypeFields {


  override def genotypesExpr: Expression = genotypes

  override def genotypeFieldsRequired: Seq[StructField] = Seq(VariantSchemas.genotypeField)

  override def child: Expression = genotypes

  override def dataType: DataType = ArrayType(IntegerType)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val fn = "com.databricks.hls.tertiary.VariantUtilExprs.genotypeStates"
    nullSafeCodeGen(ctx, ev, calls => {
      s"""
         |${ev.value} = $fn($calls, ${genotypeStructSize}, ${genotypeFieldIndices.head});
       """.stripMargin
    })
  }

  override def nullSafeEval(input: Any): Any = {
    VariantUtilExprs.genotypeStates(
      input.asInstanceOf[ArrayData],
      genotypeStructSize,
      genotypeFieldIndices.head
    )
  }
}
