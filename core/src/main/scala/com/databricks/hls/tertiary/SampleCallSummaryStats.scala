package com.databricks.hls.tertiary

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.hls.common.HLSLogging
import com.databricks.vcf.VariantSchemas

/**
 * Computes summary statistics per-sample in a genomic cohort. These statistics include the call
 * rate and the number of different types of variants.
 *
 * The return type is a map of sampleId -> summary statistics.
 */
case class CallSummaryStats(
    genotypes: Expression,
    refAllele: Expression,
    altAlleles: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[mutable.ArrayBuffer[SampleCallStats]]
    with ExpectsGenotypeFields
    with HLSLogging {

  override def genotypesExpr: Expression = genotypes

  override def genotypeFieldsRequired: Seq[StructField] = {
    Seq(VariantSchemas.callsField, VariantSchemas.sampleIdField)
  }
  override def children: Seq[Expression] = Seq(genotypes, refAllele, altAlleles)
  override def nullable: Boolean = false

  override def dataType: DataType = MapType(StringType, SampleCallStats.outputSchema)

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
    val keys = new GenericArrayData(buffer.map(s => UTF8String.fromString(s.sampleId)))
    val values = new GenericArrayData(buffer.map(_.toInternalRow))
    new ArrayBasedMapData(keys, values)
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
        .getStruct(j, genotypeStructSize)

      // Make sure the buffer has an entry for this sample
      if (j >= buffer.size) {
        val sampleId = struct
          .getUTF8String(genotypeFieldIndices(1))
        buffer.append(SampleCallStats(sampleId.toString))
      }

      val stats = buffer(j)
      val calls = struct
        .getStruct(genotypeFieldIndices.head, 2)
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
    var sampleId: String,
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

  def this() = {
    this(null)
  }

  def toInternalRow: InternalRow =
    new GenericInternalRow(
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
    )
}

object SampleCallStats extends HLSLogging {
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

  val outputSchema = StructType(
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
