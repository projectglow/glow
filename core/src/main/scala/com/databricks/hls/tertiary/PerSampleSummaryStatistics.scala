package com.databricks.hls.tertiary

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.hls.common.HLSLogging
import com.databricks.vcf.VariantSchemas

case class SampleSummaryStatsState(var sampleId: String, var momentAggState: MomentAggState) {
  def this() = this(null, null) // need 0-arg constructor for serialization
}

/**
 * Computes summary statistics (count, min, max, mean, stdev) for a numeric genotype field for each
 * sample in a cohort. The field is determined by the provided [[StructField]]. If the field does
 * not exist in the genotype struct, an analysis error will be thrown.
 *
 * The return type is a map of sampleId -> summary statistics.
 */
case class PerSampleSummaryStatistics(
    genotypes: Expression,
    field: StructField,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[mutable.ArrayBuffer[SampleSummaryStatsState]]
    with ExpectsGenotypeFields
    with HLSLogging {

  override def children: Seq[Expression] = Seq(genotypes)
  override def nullable: Boolean = false
  override def dataType: DataType = MapType(StringType, MomentAggState.schema)

  override def genotypesExpr: Expression = genotypes
  override def genotypeFieldsRequired: Seq[StructField] = Seq(VariantSchemas.sampleIdField, field)

  override def createAggregationBuffer(): ArrayBuffer[SampleSummaryStatsState] = {
    mutable.ArrayBuffer[SampleSummaryStatsState]()
  }

  override def eval(buffer: ArrayBuffer[SampleSummaryStatsState]): Any = {
    val keys = new GenericArrayData(buffer.map(s => UTF8String.fromString(s.sampleId)))
    val values = new GenericArrayData(buffer.map(s => s.momentAggState.toInternalRow))
    new ArrayBasedMapData(keys, values)
  }

  private lazy val updateStateFn: (MomentAggState, InternalRow) => Unit = {
    field.dataType match {
      case FloatType =>
        (state, genotype) => {
          state.update(genotype.getFloat(genotypeFieldIndices(1)))
        }
      case DoubleType =>
        (state, genotype) => {
          state.update(genotype.getDouble(genotypeFieldIndices(1)))
        }
      case IntegerType =>
        (state, genotype) => {
          state.update(genotype.getInt(genotypeFieldIndices(1)))
        }
      case LongType =>
        (state, genotype) => {
          state.update(genotype.getLong(genotypeFieldIndices(1)))
        }
    }
  }

  override def update(
      buffer: ArrayBuffer[SampleSummaryStatsState],
      input: InternalRow): ArrayBuffer[SampleSummaryStatsState] = {
    val genotypesArray = genotypes.eval(input).asInstanceOf[ArrayData]

    var i = 0
    while (i < genotypesArray.numElements()) {

      // Make sure the buffer has an entry for this sample
      if (i >= buffer.size) {
        val sampleId = genotypesArray
          .getStruct(buffer.size, genotypeStructSize)
          .getString(genotypeFieldIndices.head)
        buffer.append(SampleSummaryStatsState(sampleId, MomentAggState()))
      }

      val struct = genotypesArray.getStruct(i, genotypeStructSize)
      if (!struct.isNullAt(genotypeFieldIndices(1))) {
        updateStateFn(buffer(i).momentAggState, genotypesArray.getStruct(i, genotypeStructSize))
      }
      i += 1
    }
    buffer
  }

  override def merge(
      buffer: ArrayBuffer[SampleSummaryStatsState],
      input: ArrayBuffer[SampleSummaryStatsState]): ArrayBuffer[SampleSummaryStatsState] = {
    if (buffer.isEmpty) {
      return input
    } else if (input.isEmpty) {
      return buffer
    }

    require(
      buffer.size == input.size,
      s"Agg buffers have different lengths (${buffer.size}, ${input.size})"
    )
    var i = 0
    while (i < buffer.size) {
      require(buffer(i).sampleId == input(i).sampleId, s"Samples did not match at position $i")
      buffer(i).momentAggState =
        MomentAggState.merge(buffer(i).momentAggState, input(i).momentAggState)
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

  override def serialize(buffer: ArrayBuffer[SampleSummaryStatsState]): Array[Byte] = {
    SparkEnv.get.serializer.newInstance().serialize(buffer).array()
  }

  override def deserialize(storageFormat: Array[Byte]): ArrayBuffer[SampleSummaryStatsState] = {
    SparkEnv.get.serializer.newInstance().deserialize(ByteBuffer.wrap(storageFormat))
  }
}
