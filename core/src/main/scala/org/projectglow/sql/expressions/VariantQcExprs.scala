package org.projectglow.sql.expressions

import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, Cast, CreateNamedStruct, ExpectsInputTypes, Expression, GenericInternalRow, LambdaFunction, Literal, NamedLambdaVariable, UnaryExpression, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.types._

import org.projectglow.common.{GlowLogging, VCFRow, VariantSchemas}
import org.projectglow.common.{GlowLogging, VCFRow, VariantSchemas}
import org.projectglow.sql.util.LeveneHaldane
import org.projectglow.sql.util.{ExpectsGenotypeFields, LeveneHaldane}

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
   * a specific schema without requiring that the [[VCFRow]] schema remain
   * fixed for all time.
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

case class HardyWeinberg(genotypes: Expression) extends UnaryExpression with ExpectsGenotypeFields {
  override def dataType: DataType =
    StructType(
      Seq(
        StructField("hetFreqHwe", DoubleType),
        StructField("pValueHwe", DoubleType)
      )
    )

  override def genotypesExpr: Expression = genotypes

  override def genotypeFieldsRequired: Seq[StructField] = Seq(VariantSchemas.callsField)

  override def child: Expression = genotypes

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val fn = "org.projectglow.sql.expressions.VariantQcExprs.hardyWeinberg"
    nullSafeCodeGen(ctx, ev, calls => {
      s"""
         |${ev.value} = $fn($calls, $genotypeStructSize, ${genotypeFieldIndices.head});
       """.stripMargin
    })
  }

  override def nullSafeEval(input: Any): Any = {
    VariantQcExprs.hardyWeinberg(
      input.asInstanceOf[ArrayData],
      genotypeStructSize,
      genotypeFieldIndices.head
    )
  }
}

object HardyWeinberg {
  lazy val schema: DataType = ScalaReflection.schemaFor[CallStats].dataType
}

case class HardyWeinbergStruct(hetFreqHwe: Double, pValueHwe: Double)

case class CallStats(genotypes: Expression) extends UnaryExpression with ExpectsGenotypeFields {
  lazy val dataType: DataType = CallStats.schema

  override def genotypesExpr: Expression = genotypes

  override def genotypeFieldsRequired: Seq[StructField] = Seq(VariantSchemas.callsField)

  override def child: Expression = genotypes

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val fn = "org.projectglow.sql.expressions.VariantQcExprs.callStats"
    nullSafeCodeGen(ctx, ev, calls => {
      s"""
         |${ev.value} = $fn($calls, $genotypeStructSize, ${genotypeFieldIndices.head});
       """.stripMargin
    })
  }

  override def nullSafeEval(input: Any): Any = {
    VariantQcExprs.callStats(
      input.asInstanceOf[ArrayData],
      genotypeStructSize,
      genotypeFieldIndices.head
    )
  }
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

case class ArrayStatsSummary(array: Expression) extends UnaryExpression with ExpectsInputTypes {
  override def child: Expression = array

  override def inputTypes = Seq(ArrayType(DoubleType)) // scalastyle:ignore

  override def dataType: StructType = MomentAggState.schema

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, c => {
      s"""
         |${ev.value} = org.projectglow.sql.expressions.VariantQcExprs.arraySummaryStats($c);
       """.stripMargin
    })
  }

  override def nullSafeEval(input: Any): Any = {
    VariantQcExprs.arraySummaryStats(input.asInstanceOf[ArrayData])
  }
}

object ArrayStatsSummary {
  def extractDoubleArray(fieldName: String, child: Expression): Expression = {
    val lambdaVar = UnresolvedNamedLambdaVariable(Seq("g"))
    val function =
      Cast(UnresolvedExtractValue(lambdaVar, Literal(fieldName)), DoubleType)
    val lambdaFunction = LambdaFunction(function, Seq(lambdaVar), hidden = false)
    ArrayTransform(child, lambdaFunction)
  }

  def makeDpStats(child: Expression): Expression = {
    ArrayStatsSummary(extractDoubleArray("depth", child))
  }

  def makeGqStats(child: Expression): Expression = {
    ArrayStatsSummary(extractDoubleArray("conditionalQuality", child))
  }
}
