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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, Literal, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.common.VariantSchemas
import io.projectglow.sql.util.{ExpectsGenotypeFields, GenotypeInfo}

/**
 * Implementations of utility functions for transforming variant representations. These
 * implementations are called during both whole-stage codegen and interpreted execution.
 *
 * The functions are exposed to the user as Catalyst expressions.
 */
object VariantUtilExprs {
  def genotypeStates(genotypes: ArrayData, genotypesSize: Int, genotypesIdx: Int): ArrayData = {
    val output = ArrayData.allocateArrayData(IntegerType.defaultSize, genotypes.numElements(), "")
    var i = 0
    while (i < output.numElements()) {
      val calls = genotypes
        .getStruct(i, genotypesSize)
        .getArray(genotypesIdx)
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
      output.setInt(i, if (j == 0) -1 else sum)
      i += 1
    }

    output
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

  def containsTransition(refAllele: UTF8String, altAllele: UTF8String): Boolean = {
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

  def containsTransversion(refAllele: UTF8String, altAllele: UTF8String): Boolean = {
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
    if (isSnp(refAllele, altAllele) && containsTransition(refAllele, altAllele)) {
      VariantType.Transition
    } else if (isSnp(refAllele, altAllele) && containsTransversion(refAllele, altAllele)) {
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
case class GenotypeStates(genotypes: Expression, genotypeInfo: Option[GenotypeInfo])
    extends UnaryExpression
    with ExpectsGenotypeFields {

  def this(genotypes: Expression) = this(genotypes, None)

  override def genotypesExpr: Expression = genotypes

  override def requiredGenotypeFields: Seq[StructField] = Seq(VariantSchemas.callsField)

  override def withGenotypeInfo(genotypeInfo: GenotypeInfo): GenotypeStates = {
    copy(genotypes, Some(genotypeInfo))
  }

  override def child: Expression = genotypes

  override def dataType: DataType = ArrayType(IntegerType)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val fn = "io.projectglow.sql.expressions.VariantUtilExprs.genotypeStates"
    nullSafeCodeGen(
      ctx,
      ev,
      calls => {
        s"""
         |${ev.value} = $fn($calls, ${getGenotypeInfo.size}, ${getGenotypeInfo
             .requiredFieldIndices
             .head});
       """.stripMargin
      }
    )
  }

  override def nullSafeEval(input: Any): Any = {
    VariantUtilExprs.genotypeStates(
      input.asInstanceOf[ArrayData],
      getGenotypeInfo.size,
      getGenotypeInfo.requiredFieldIndices.head
    )
  }
}

/**
 * Converts an array of probabilities (most likely the genotype probabilities from a BGEN file)
 * into hard calls. The input probabilities are assumed to be diploid.
 *
 * If the input probabilities are phased, each haplotype is called separately by finding the maximum
 * probability greater than the threshold (0.9 by default, a la plink). If no probability is
 * greater than the threshold, the call is -1 (missing).
 *
 * If the input probabilities are unphased, the probabilities refer to the complete genotype. In
 * this case, we find the maximum probability greater than the threshold and then convert that
 * value to a genotype call.
 *
 * If any of the required parameters (probabilities, numAlts, phased) are null, the expression
 * returns null.
 *
 * @param probabilities The probabilities to convert to hard calls. The algorithm does not check
 *                      that they sum to 1. If the probabilities are unphased, they are assumed
 *                      to correspond to the genotypes in colex order, which is standard for both
 *                      BGEN and VCF files.
 * @param numAlts The number of alternate alleles at this site.
 * @param phased Whether the probabilities are phased (per haplotype) or unphased (whole genotype).
 * @param threshold Calls are only generated if at least one probability is above this threshold.
 */
case class HardCalls(
    probabilities: Expression,
    numAlts: Expression,
    phased: Expression,
    threshold: Expression)
    extends CodegenFallback
    with ImplicitCastInputTypes {

  def this(probabilities: Expression, numAlts: Expression, phased: Expression) = {
    this(probabilities, numAlts, phased, Literal(0.90d))
  }

  override def children: Seq[Expression] = Seq(probabilities, numAlts, phased, threshold)
  override def inputTypes = { // scalastyle:ignore
    Seq(ArrayType(DoubleType), IntegerType, BooleanType, DecimalType)
  }
  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes()
    if (!threshold.foldable) {
      TypeCheckResult.TypeCheckFailure("Threshold must be a constant value")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = ArrayType(IntegerType)
  override def nullable: Boolean = probabilities.nullable || numAlts.nullable || phased.nullable
  private lazy val threshold0 = threshold.eval().asInstanceOf[Decimal].toDouble

  override def eval(input: InternalRow): Any = {
    val _probArr = probabilities.eval(input)
    val _numAlts = numAlts.eval(input)
    val _phased0 = phased.eval(input)
    if (_probArr == null || _numAlts == null || _phased0 == null) {
      return null
    }

    val probArr = _probArr.asInstanceOf[ArrayData]
    val numAlleles = _numAlts.asInstanceOf[Int] + 1
    val phased0 = _phased0.asInstanceOf[Boolean]

    // calls is an `Array[Any]` instead of `Array[Int]` because it's cheaper to convert
    // the former to Spark's array data format
    // phased case
    val calls: Array[Any] = if (phased0) {
      val out = new Array[Any](2) // 2 because we assume diploid
      var i = 0
      while (i < 2) {
        var j = 0
        var max = 0d
        var call = -1
        while (j < numAlleles) {
          val probability = probArr.getDouble(i * numAlleles + j)
          if (probability >= threshold0 && probability > max) {
            max = probability
            call = j
          }
          j += 1
        }
        out(i) = call
        i += 1
      }
      out
    } else { // unphased case
      var i = 0
      var maxProb = 0d
      var maxIdx = -1
      while (i < probArr.numElements()) {
        val el = probArr.getDouble(i)
        if (el >= threshold0 && el > maxProb) {
          maxIdx = i
          maxProb = el
        }
        i += 1
      }
      callsFromIdx(maxIdx)
    }

    new GenericArrayData(calls)
  }

  /**
   * Converts the index of the maximum probability in an unphased probability array into diploid
   * genotype calls. Since the probabilities correspond to genotypes enumerated in colex order,
   * we can think of this function as mapping the index to a specific genotype in an infinite
   * sequence of possible diploid genotypes.
   *
   * 00
   * 01
   * 11
   * 02
   * 12
   * 22
   * 03
   * 13
   * 23
   * 33
   * ..
   *
   * In the general ploidy case, this mapping is the combinatorial number system with replacement.
   * In the diploid case, the higher numbered call is equivalent to the index of the greatest
   * triangle number less than `maxIdx`. The lower number call is then the difference between that
   * triangle number and the `maxIdx`.
   */
  private def callsFromIdx(maxIdx: Int): Array[Any] = {
    if (maxIdx == -1) {
      return Array(-1, -1)
    }

    val calls = new Array[Any](2) // 2 because we assume diploid
    var sum = 0
    var i = 0
    while (sum + i + 1 <= maxIdx) {
      i += 1
      sum += i
    }

    calls(0) = i
    calls(1) = maxIdx - sum
    calls
  }
}
