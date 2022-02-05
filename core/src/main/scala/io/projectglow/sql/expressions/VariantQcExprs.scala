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

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, JavaCode, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, BinaryExpression, Cast, CreateNamedStruct, ExpectsInputTypes, Expression, GenericInternalRow, ImplicitCastInputTypes, LambdaFunction, Literal, NamedLambdaVariable, UnaryExpression, Unevaluable, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.common.{GlowLogging, VariantSchemas}
import io.projectglow.sql.util.{ExpectsGenotypeFields, GenotypeInfo, LeveneHaldane, Rewrite}

/**
 * Contains implementations of QC functions. These implementations are called during both
 * whole-stage codegen and interpreted execution.
 *
 * The functions are exposed to the user as Catalyst expressions.
 */
object VariantQcExprs extends GlowLogging {

  /**
   * Performs a two-sided test of the Hardy-Weinberg equilibrium. Returns the expected het frequency
   * as well as the associated p value.
   * @param genotypes an array of structs with the schema required by [[CallStats]]
   * @param genotypeIdx the position of the genotype struct (with calls and phasing info) within
   *                     the element struct of the genotypes array
   * @return a row with the schema of [[HardyWeinbergStruct]]
   */
  def hardyWeinberg(
      genotypes: ArrayData,
      genotypesSize: Int,
      genotypeIdx: Int): GenericInternalRow = {
    val callStats = callStatsBase(genotypes, genotypesSize, genotypeIdx)

    val nHomRef = if (callStats.nHomozygous.nonEmpty) callStats.nHomozygous.head else 0
    val nHomAlt = if (callStats.nHomozygous.length > 1) callStats.nHomozygous(1) else 0
    val n = callStats.nHet + nHomAlt + nHomRef
    val dist = LeveneHaldane(n, callStats.nHet + 2 * Math.min(nHomRef, nHomAlt))
    val output = Array(
      (dist.getNumericalMean / callStats.nCalled): java.lang.Double,
      dist.exactMidP(callStats.nHet): java.lang.Double
    )

    new GenericInternalRow(output.asInstanceOf[Array[Any]])
  }

  /**
   * Calculates a variety of summary stats on the calls for a given site. This method returns
   * a case class so that the output can be used easily from other QC functions as well as
   * returned directly to the user.
   *
   * @param genotypes an array of structs with the schema defined in [[CallStats.requiredSchema]]
   * @param genotypesIdx the position of the calls within the element struct of the genotypes array
   */
  def callStatsBase(
      genotypes: ArrayData,
      genotypesSize: Int,
      genotypesIdx: Int): CallStatsStruct = {
    var i = 0
    var nCalled = 0
    var nUncalled = 0
    var nHet = 0
    var nNonRef = 0
    val homozygotes = new java.util.ArrayList[Integer](2)
    val alleleCounts = new java.util.ArrayList[Integer](2)
    var alleleN = 0
    while (i < genotypes.numElements()) {
      val calls = genotypes
        .getStruct(i, genotypesSize)
        .getArray(genotypesIdx)
        .toIntArray()
      var isHet = false
      var isUncalled = false
      var lastAllele = -1
      var isNonRef = false
      var j = 0
      while (j < calls.length) {
        if (calls(j) == -1) {
          isUncalled = true
        } else {
          while (alleleCounts.size - 1 < calls(j)) {
            alleleCounts.add(0)
          }
          alleleCounts.set(calls(j), alleleCounts.get(calls(j)) + 1)
          alleleN += 1

          if (lastAllele != -1 && calls(j) != lastAllele) {
            isHet = true
          }
          lastAllele = calls(j)

          if (calls(j) > 0) {
            isNonRef = true
          }

        }
        j += 1
      }

      if (isNonRef) {
        nNonRef += 1
      }

      if (j == 0 || isUncalled) {
        nUncalled += 1
      } else {
        nCalled += 1
      }

      if (isHet) {
        nHet += 1
      } else if (lastAllele != -1) {
        while (homozygotes.size - 1 < lastAllele) {
          homozygotes.add(0)
        }
        homozygotes.set(lastAllele, homozygotes.get(lastAllele) + 1)
      }
      i += 1
    }

    val homozygotesArr = new Array[Int](homozygotes.size())
    var idx = 0
    // Unfortunately, we have a build an array ourselves instead of calling List.toArray
    // because of issues with primitive boxing
    while (idx < homozygotes.size()) {
      homozygotesArr(idx) = homozygotes.get(idx)
      idx += 1
    }

    val alleleFrequency = new Array[Double](alleleCounts.size())
    val alleleCountsArr = new Array[Int](alleleCounts.size())
    idx = 0
    while (idx < alleleCounts.size()) {
      alleleCountsArr(idx) = alleleCounts.get(idx)
      alleleFrequency(idx) = alleleCounts.get(idx).toDouble / alleleN
      idx += 1
    }

    CallStatsStruct(
      nCalled.toDouble / (nCalled + nUncalled),
      nCalled,
      nUncalled,
      nHet,
      homozygotesArr,
      nNonRef,
      alleleN,
      alleleCountsArr,
      alleleFrequency
    )
  }

  def callStats(genotypes: ArrayData, genotypesSize: Int, genotypeIdx: Int): InternalRow = {
    val base = callStatsBase(genotypes, genotypesSize, genotypeIdx)
    new GenericInternalRow(
      Array(
        base.callRate,
        base.nCalled,
        base.nUncalled,
        base.nHet,
        new GenericArrayData(base.nHomozygous),
        base.nNonRef,
        base.nAllelesCalled,
        new GenericArrayData(base.alleleCounts),
        new GenericArrayData(base.alleleFrequencies)
      )
    )
  }

  /**
   * Calculates basic summary stats (min, max, mean, sample stddev) on an array of double
   * typed values. These are calculated using a one pass algorithm described in
   * https://arxiv.org/abs/1510.04923
   *
   * The algorithm used is adapted from
   * [[org.apache.spark.sql.catalyst.expressions.aggregate.CentralMomentAgg]]
   * @param arrayData
   * @return
   */
  def arraySummaryStats(arrayData: ArrayData): InternalRow = {
    var i = 0
    val state = MomentAggState()
    while (i < arrayData.numElements()) {
      if (!arrayData.isNullAt(i)) {
        state.update(arrayData.getDouble(i))
      }
      i += 1
    }
    state.toInternalRow
  }

  /**
   * Converts an array of struct-typed expressions into a slimmed down struct with a subset of
   * the fields.
   *
   * We use this function for many of the variant QC functions so that each function can require
   * a specific schema.
   *
   * @param schema the desired schema
   * @param expr an array of struct-typed expressions that contains a superset of the fields in
   *             `schema`
   * @return a transformed array of struct-typed expressions with the schema of `schema`
   */
  def subsetExpr(schema: StructType, expr: Expression): Expression = {
    val (dataType, nullable) = expr.dataType match {
      case ArrayType(dt, isNullable) => (dt, isNullable)
      case _ => throw new UnsupportedOperationException("input must be an array of structs")
    }
    val fieldNameExprs = schema.fieldNames.map(Literal(_))
    val arg = NamedLambdaVariable("genotype", dataType, nullable)
    val fn = CreateNamedStruct(fieldNameExprs.flatMap { f =>
      Seq(f, UnresolvedExtractValue(arg, f))
    })

    ArrayTransform(expr, LambdaFunction(fn, Seq(arg)))
  }
}

case class HardyWeinberg(genotypes: Expression, genotypeInfo: Option[GenotypeInfo])
    extends UnaryExpression
    with ExpectsGenotypeFields {
  def this(genotypes: Expression) = this(genotypes, None)

  override def dataType: DataType =
    StructType(
      Seq(
        StructField("hetFreqHwe", DoubleType),
        StructField("pValueHwe", DoubleType)
      )
    )

  override def genotypesExpr: Expression = genotypes

  override def requiredGenotypeFields: Seq[StructField] = Seq(VariantSchemas.callsField)

  override def withGenotypeInfo(genotypeInfo: GenotypeInfo): HardyWeinberg = {
    copy(genotypeInfo = Some(genotypeInfo))
  }

  override def child: Expression = genotypes

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val fn = "io.projectglow.sql.expressions.VariantQcExprs.hardyWeinberg"
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
    VariantQcExprs.hardyWeinberg(
      input.asInstanceOf[ArrayData],
      getGenotypeInfo.size,
      getGenotypeInfo.requiredFieldIndices.head
    )
  }

  override def withNewChildInternal(newChild: Expression): HardyWeinberg = {
    copy(genotypes = newChild)
  }
}

object HardyWeinberg {
  lazy val schema: DataType = ScalaReflection.schemaFor[CallStats].dataType
}

case class HardyWeinbergStruct(hetFreqHwe: Double, pValueHwe: Double)

case class CallStats(genotypes: Expression, genotypeInfo: Option[GenotypeInfo])
    extends UnaryExpression
    with ExpectsGenotypeFields {
  def this(genotypes: Expression) = this(genotypes, None)

  lazy val dataType: DataType = CallStats.schema

  override def genotypesExpr: Expression = genotypes

  override def requiredGenotypeFields: Seq[StructField] = Seq(VariantSchemas.callsField)

  override def withGenotypeInfo(genotypeInfo: GenotypeInfo): CallStats = {
    copy(genotypeInfo = Some(genotypeInfo))
  }

  override def child: Expression = genotypes

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val fn = "io.projectglow.sql.expressions.VariantQcExprs.callStats"
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
    VariantQcExprs.callStats(
      input.asInstanceOf[ArrayData],
      getGenotypeInfo.size,
      getGenotypeInfo.requiredFieldIndices.head
    )
  }

  override protected def withNewChildInternal(newChild: Expression): CallStats =
    copy(genotypes = newChild)
}

case class CallStatsStruct(
    callRate: Double,
    nCalled: Int,
    nUncalled: Int,
    nHet: Int,
    nHomozygous: Array[Int],
    nNonRef: Int,
    nAllelesCalled: Int,
    alleleCounts: Array[Int],
    alleleFrequencies: Array[Double])

object CallStats {
  lazy val schema: DataType = ScalaReflection.schemaFor[CallStatsStruct].dataType

  lazy val requiredSchema: StructType = StructType(Seq(VariantSchemas.callsField))
}

case class ArrayStatsSummary(array: Expression)
    extends UnaryExpression
    with ImplicitCastInputTypes {
  override def child: Expression = array

  override def inputTypes = Seq(ArrayType(DoubleType)) // scalastyle:ignore

  override def dataType: StructType = MomentAggState.schema

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => {
      s"""
         |${ev.value} = io.projectglow.sql.expressions.VariantQcExprs.arraySummaryStats($c);
       """.stripMargin
    })
  }

  override def nullSafeEval(input: Any): Any = {
    VariantQcExprs.arraySummaryStats(input.asInstanceOf[ArrayData])
  }

  override protected def withNewChildInternal(newChild: Expression): ArrayStatsSummary =
    copy(array = newChild)
}

case class DpSummaryStats(child: Expression) extends Rewrite {
  override def children: Seq[Expression] = Seq(child)
  override def rewrite: Expression = {
    ArrayStatsSummary(UnresolvedExtractValue(child, Literal(VariantSchemas.depthField.name)))
  }

  override protected def withNewChildrenInternal(
      children: IndexedSeq[Expression]): DpSummaryStats = {
    copy(child = children.head)
  }
}

case class GqSummaryStats(child: Expression) extends Rewrite {
  override def children: Seq[Expression] = Seq(child)
  override def rewrite: Expression = {
    ArrayStatsSummary(
      UnresolvedExtractValue(child, Literal(VariantSchemas.conditionalQualityField.name)))
  }

  override protected def withNewChildrenInternal(
      children: IndexedSeq[Expression]): GqSummaryStats = {
    copy(child = children.head)
  }
}

// Mostly a copy of Spark's AssertTrue, but with an additional parameter to customize the error message
case class AssertTrueOrError(child: Expression, errMsg: Expression)
    extends BinaryExpression
    with ImplicitCastInputTypes {
  override def left: Expression = child
  override def right: Expression = errMsg
  override def nullable: Boolean = true
  override def inputTypes: Seq[DataType] = Seq(BooleanType, StringType)
  override def dataType: DataType = NullType

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes()
    if (!errMsg.foldable) {
      TypeCheckResult.TypeCheckFailure("Error message must be a constant value")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  private lazy val errMsg0 = errMsg.eval().asInstanceOf[UTF8String].toString()

  override def eval(input: InternalRow): Any = {
    val v = child.eval(input)
    if (v == null || java.lang.Boolean.FALSE.equals(v)) {
      throw new RuntimeException(errMsg0)
    } else {
      null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)

    // Use unnamed reference that doesn't create a local field here to reduce the number of fields
    // because errMsgField is used only when the value is null or false.
    val errMsgField = ctx.addReferenceObj("errMsg", errMsg0)
    ExprCode(
      code = code"""${eval.code}
                          |if (${eval.isNull} || !${eval.value}) {
                          |  throw new RuntimeException($errMsgField);
                          |}""".stripMargin,
      isNull = TrueLiteral,
      value = JavaCode.defaultLiteral(dataType)
    )
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): AssertTrueOrError = {
    copy(child = newLeft, errMsg = newRight)
  }
}
