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

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.common.{GlowLogging, VariantSchemas}
import io.projectglow.sql.util.{ExpectsGenotypeFields, GenotypeInfo, Rewrite}

case class SampleSummaryStatsState(var sampleId: String, var momentAggState: MomentAggState) {
  def this() = this(null, null) // need 0-arg constructor for serialization
}

/**
 * Computes summary statistics (count, min, max, mean, stdev) for a numeric genotype field for each
 * sample in a cohort. The field is determined by the provided [[StructField]]. If the field does
 * not exist in the genotype struct, an analysis error will be thrown.
 *
 * The return type is an array of summary statistics. If sample ids are included in the input,
 * they'll be propagated to the results.
 */
case class PerSampleSummaryStatistics(
    genotypes: Expression,
    field: Expression,
    genotypeInfo: Option[GenotypeInfo] = None,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[mutable.ArrayBuffer[SampleSummaryStatsState]]
    with ExpectsGenotypeFields
    with GlowLogging {

  override def children: Seq[Expression] = Seq(genotypes)
  override def nullable: Boolean = false

  override def dataType: DataType =
    if (hasSampleIds) {
      val fields = VariantSchemas.sampleIdField +: MomentAggState.schema.fields
      ArrayType(StructType(fields))
    } else {
      ArrayType(MomentAggState.schema)
    }

  override def genotypesExpr: Expression = genotypes
  override def requiredGenotypeFields: Seq[StructField] = {
    if (!field.foldable || field.dataType != StringType) {
      throw SQLUtils.newAnalysisException("Field must be foldable string")
    }
    val fieldName = field.eval().asInstanceOf[UTF8String].toString
    if (fieldName == VariantSchemas.conditionalQualityField.name) {
      Seq(VariantSchemas.conditionalQualityField)
    } else if (fieldName == VariantSchemas.depthField.name) {
      Seq(VariantSchemas.depthField)
    } else {
      throw SQLUtils.newAnalysisException("Unsupported field")
    }
  }

  override def optionalGenotypeFields: Seq[StructField] = Seq(VariantSchemas.sampleIdField)

  override def withGenotypeInfo(genotypeInfo: GenotypeInfo): PerSampleSummaryStatistics = {
    copy(genotypeInfo = Some(genotypeInfo))
  }
  private lazy val hasSampleIds = getGenotypeInfo.optionalFieldIndices(0) != -1

  override def createAggregationBuffer(): ArrayBuffer[SampleSummaryStatsState] = {
    mutable.ArrayBuffer[SampleSummaryStatsState]()
  }

  override def eval(buffer: ArrayBuffer[SampleSummaryStatsState]): Any = {
    if (!hasSampleIds) {
      new GenericArrayData(buffer.map(s => s.momentAggState.toInternalRow))
    } else {
      new GenericArrayData(buffer.map { s =>
        val outputRow = new GenericInternalRow(MomentAggState.schema.length + 1)
        outputRow.update(0, UTF8String.fromString(s.sampleId))
        s.momentAggState.toInternalRow(outputRow, offset = 1)
      })
    }
  }

  private lazy val updateStateFn: (MomentAggState, InternalRow) => Unit = {
    requiredGenotypeFields.head.dataType match {
      case FloatType =>
        (state, genotype) => {
          state.update(genotype.getFloat(getGenotypeInfo.requiredFieldIndices(0)))
        }
      case DoubleType =>
        (state, genotype) => {
          state.update(genotype.getDouble(getGenotypeInfo.requiredFieldIndices(0)))
        }
      case IntegerType =>
        (state, genotype) => {
          state.update(genotype.getInt(getGenotypeInfo.requiredFieldIndices(0)))
        }
      case LongType =>
        (state, genotype) => {
          state.update(genotype.getLong(getGenotypeInfo.requiredFieldIndices(0)))
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
        val sampleId = if (hasSampleIds) {
          genotypesArray
            .getStruct(buffer.size, getGenotypeInfo.size)
            .getString(getGenotypeInfo.optionalFieldIndices(0))
        } else {
          null
        }
        buffer.append(SampleSummaryStatsState(sampleId, MomentAggState()))
      }

      val struct = genotypesArray.getStruct(i, getGenotypeInfo.size)
      if (!struct.isNullAt(getGenotypeInfo.requiredFieldIndices(0))) {
        updateStateFn(buffer(i).momentAggState, genotypesArray.getStruct(i, getGenotypeInfo.size))
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
      require(
        buffer(i).sampleId == input(i).sampleId,
        s"Samples did not match at position $i (${buffer(i).sampleId}, ${input(i).sampleId})")
      buffer(i).momentAggState =
        MomentAggState.merge(buffer(i).momentAggState, input(i).momentAggState)
      i += 1
    }
    buffer
  }

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): PerSampleSummaryStatistics = {
    copy(inputAggBufferOffset = newInputAggBufferOffset)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): PerSampleSummaryStatistics = {
    copy(mutableAggBufferOffset = newOffset)
  }

  override def serialize(buffer: ArrayBuffer[SampleSummaryStatsState]): Array[Byte] = {
    SparkEnv.get.serializer.newInstance().serialize(buffer).array()
  }

  override def deserialize(storageFormat: Array[Byte]): ArrayBuffer[SampleSummaryStatsState] = {
    SparkEnv.get.serializer.newInstance().deserialize(ByteBuffer.wrap(storageFormat))
  }
}

case class SampleDpSummaryStatistics(child: Expression) extends Rewrite {
  override def children: Seq[Expression] = Seq(child)
  override def rewrite: Expression = {
    PerSampleSummaryStatistics(child, Literal(VariantSchemas.depthField.name))
      .toAggregateExpression()
  }
}

case class SampleGqSummaryStatistics(child: Expression) extends Rewrite {
  override def children: Seq[Expression] = Seq(child)
  override def rewrite: Expression = {
    PerSampleSummaryStatistics(child, Literal(VariantSchemas.conditionalQualityField.name))
      .toAggregateExpression()
  }
}
