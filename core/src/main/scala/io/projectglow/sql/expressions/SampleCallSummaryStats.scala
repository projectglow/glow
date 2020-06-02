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

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import io.projectglow.common.{GlowLogging, VariantSchemas}
import io.projectglow.sql.util.{ExpectsGenotypeFields, GenotypeInfo}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Computes summary statistics per-sample in a genomic cohort. These statistics include the call
 * rate and the number of different types of variants.
 *
 * The return type is an array of summary statistics. If sample ids are included in the input
 * schema, they'll be propagated to the results.
 */
case class CallSummaryStats(
    genotypes: Expression,
    refAllele: Expression,
    altAlleles: Expression,
    genotypeInfo: Option[GenotypeInfo],
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[mutable.ArrayBuffer[SampleCallStats]]
    with ExpectsGenotypeFields
    with GlowLogging {

  def this(genotypes: Expression, refAllele: Expression, altAlleles: Expression) = {
    this(genotypes, refAllele, altAlleles, None, 0, 0)
  }

  override def genotypesExpr: Expression = genotypes

  override def requiredGenotypeFields: Seq[StructField] = {
    Seq(VariantSchemas.callsField)
  }

  override def optionalGenotypeFields: Seq[StructField] = Seq(VariantSchemas.sampleIdField)

  override def withGenotypeInfo(genotypeInfo: GenotypeInfo): CallSummaryStats = {
    copy(genotypeInfo = Some(genotypeInfo))
  }
  private lazy val hasSampleIds = getGenotypeInfo.optionalFieldIndices(0) != -1
  override def children: Seq[Expression] = Seq(genotypes, refAllele, altAlleles)
  override def nullable: Boolean = false

  override def dataType: DataType =
    ArrayType(SampleCallStats.outputSchema(hasSampleIds))

  override def checkInputDataTypes(): TypeCheckResult = {
    if (super.checkInputDataTypes().isFailure) {
      return super.checkInputDataTypes()
    }

    val errors = mutable.ArrayBuffer[String]()
    if (refAllele.dataType != StringType) {
      errors.append("Reference allele must be a string")
    }

    altAlleles.dataType match {
      case a: ArrayType if a.elementType == StringType => // nothing to do
      case _ => errors.append("Alternate alleles must be an array of strings")
    }

    if (errors.isEmpty) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(s"Type check failure: (${errors.mkString(", ")})")
    }
  }

  override def createAggregationBuffer(): mutable.ArrayBuffer[SampleCallStats] = {
    mutable.ArrayBuffer[SampleCallStats]()
  }

  override def eval(buffer: ArrayBuffer[SampleCallStats]): Any = {
    new GenericArrayData(buffer.map(_.toInternalRow(hasSampleIds)))
  }

  override def update(
      buffer: ArrayBuffer[SampleCallStats],
      input: InternalRow): ArrayBuffer[SampleCallStats] = {

    val genotypesArr = genotypes.eval(input).asInstanceOf[ArrayData]
    val ref = refAllele.eval(input).asInstanceOf[UTF8String]
    val alts = altAlleles.eval(input).asInstanceOf[ArrayData]

    val alleleTypes = new Array[VariantType](alts.numElements())
    var i = 0
    while (i < alleleTypes.length) {
      val alt = alts.getUTF8String(i)
      alleleTypes(i) = VariantUtilExprs.variantType(ref, alt)
      i += 1
    }

    var j = 0
    while (j < genotypesArr.numElements()) {
      val struct = genotypesArr
        .getStruct(j, getGenotypeInfo.size)

      // Make sure the buffer has an entry for this sample
      if (j >= buffer.size) {
        val sampleId = if (hasSampleIds) {
          struct.getUTF8String(getGenotypeInfo.optionalFieldIndices(0))
        } else {
          null
        }
        buffer.append(SampleCallStats(if (sampleId != null) sampleId.toString else null))
      }

      val stats = buffer(j)
      val calls = struct
        .getStruct(getGenotypeInfo.requiredFieldIndices.head, 2)
        .getArray(0)
      var k = 0
      var isUncalled = false
      var isHet = false
      var lastAllele = -1
      while (k < calls.numElements()) {
        val call = calls.getInt(k)

        if (call == -1) {
          isUncalled = true
        } else if (call != lastAllele && lastAllele != -1) {
          isHet = true
        }

        if (call > 0) {
          if (alleleTypes(call - 1) == VariantType.Transition) {
            stats.nTransition += 1
          } else if (alleleTypes(call - 1) == VariantType.Transversion) {
            stats.nTransversion += 1
          } else if (alleleTypes(call - 1) == VariantType.Insertion) {
            stats.nInsertion += 1
          } else if (alleleTypes(call - 1) == VariantType.Deletion) {
            stats.nDeletion += 1
          } else if (alleleTypes(call - 1) == VariantType.SpanningDeletion) {
            stats.nSpanningDeletion += 1
          }
        }

        lastAllele = call
        k += 1
      }

      if (isUncalled) {
        stats.nUncalled += 1
      } else {
        stats.nCalled += 1
        if (isHet) {
          stats.nHet += 1
        } else if (lastAllele == 0) {
          stats.nHomRef += 1
        } else {
          stats.nHomVar += 1
        }
      }

      j += 1
    }

    buffer
  }

  override def merge(
      buffer: ArrayBuffer[SampleCallStats],
      input: ArrayBuffer[SampleCallStats]): ArrayBuffer[SampleCallStats] = {
    if (buffer.isEmpty) {
      return input
    } else if (input.isEmpty) {
      return buffer
    }

    require(
      buffer.size == input.size,
      s"Aggregation buffers have different lengths. ${(buffer.size, input.size)}"
    )
    var i = 0
    while (i < buffer.size) {
      buffer(i) = SampleCallStats.merge(buffer(i), input(i))
      i += 1
    }
    buffer
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate = {
    copy(mutableAggBufferOffset = newOffset)
  }

  override def serialize(buffer: ArrayBuffer[SampleCallStats]): Array[Byte] = {
    SparkEnv.get.serializer.newInstance().serialize(buffer).array()
  }

  override def deserialize(storageFormat: Array[Byte]): ArrayBuffer[SampleCallStats] = {
    SparkEnv.get.serializer.newInstance().deserialize(ByteBuffer.wrap(storageFormat))
  }
}

case class SampleCallStats(
    var sampleId: String = null,
    var nCalled: Long = 0,
    var nUncalled: Long = 0,
    var nHomRef: Long = 0,
    var nHet: Long = 0,
    var nHomVar: Long = 0,
    var nInsertion: Long = 0,
    var nDeletion: Long = 0,
    var nTransversion: Long = 0,
    var nTransition: Long = 0,
    var nSpanningDeletion: Long = 0) {

  def toInternalRow(includeSampleId: Boolean): InternalRow = {
    val valueArr =
      Array[Any](
        nCalled.toDouble / (nCalled + nUncalled),
        nCalled,
        nUncalled,
        nHomRef,
        nHet,
        nHomVar,
        nTransition + nTransversion,
        nInsertion,
        nDeletion,
        nTransition,
        nTransversion,
        nSpanningDeletion,
        nTransition.toDouble / nTransversion,
        nInsertion.toDouble / nDeletion,
        nHet.toDouble / nHomVar
      )

    if (includeSampleId) {
      new GenericInternalRow(Array[Any](UTF8String.fromString(sampleId)) ++ valueArr)
    } else {
      new GenericInternalRow(valueArr)
    }
  }
}

object SampleCallStats extends GlowLogging {
  def merge(s1: SampleCallStats, s2: SampleCallStats): SampleCallStats = {
    require(s1.sampleId == s2.sampleId, s"${s1.sampleId}, ${s2.sampleId}")
    val out = new SampleCallStats(s1.sampleId)
    out.nCalled = s1.nCalled + s2.nCalled
    out.nUncalled = s1.nUncalled + s2.nUncalled
    out.nHomRef = s1.nHomRef + s2.nHomRef
    out.nHomVar = s1.nHomVar + s2.nHomVar
    out.nHet = s1.nHet + s2.nHet
    out.nInsertion = s1.nInsertion + s2.nInsertion
    out.nDeletion = s1.nDeletion + s2.nDeletion
    out.nTransversion = s1.nTransversion + s2.nTransversion
    out.nTransition = s1.nTransition + s2.nTransition
    out.nSpanningDeletion = s1.nSpanningDeletion + s2.nSpanningDeletion
    out
  }

  private[projectglow] def outputSchema(includeSampleId: Boolean): StructType = StructType(
    (if (includeSampleId) Some(VariantSchemas.sampleIdField) else None).toSeq ++
    Seq(
      StructField("callRate", DoubleType),
      StructField("nCalled", LongType),
      StructField("nUncalled", LongType),
      StructField("nHomRef", LongType),
      StructField("nHet", LongType),
      StructField("nHomVar", LongType),
      StructField("nSnp", LongType),
      StructField("nInsertion", LongType),
      StructField("nDeletion", LongType),
      StructField("nTransition", LongType),
      StructField("nTransversion", LongType),
      StructField("nSpanningDeletion", LongType),
      StructField("rTiTv", DoubleType),
      StructField("rInsertionDeletion", DoubleType),
      StructField("rHetHomVar", DoubleType)
    )
  )
}
