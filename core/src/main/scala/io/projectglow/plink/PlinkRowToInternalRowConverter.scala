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

package io.projectglow.plink

import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.common.{GlowLogging, VariantSchemas}
import io.projectglow.sql.util.RowConverter

/**
 * Creates an [[InternalRow]] with a given required schema. During construction,
 * this class will throw an [[IllegalArgumentException]] if any of the fields in the required
 * schema cannot be derived from a PLINK record.
 */
class PlinkRowToInternalRowConverter(schema: StructType) extends GlowLogging {

  private val homAlt = new GenericArrayData(Array(1, 1))
  private val missing = new GenericArrayData(Array(-1, -1))
  private val het = new GenericArrayData(Array(0, 1))
  private val homRef = new GenericArrayData(Array(0, 0))

  private def twoBitsToCalls(twoBits: Int): GenericArrayData = {
    twoBits match {
      case 0 => homAlt // Homozygous for first (alternate) allele
      case 1 => missing // Missing genotype
      case 2 => het // Heterozygous
      case 3 => homRef // Homozygous for second (reference) allele
    }
  }

  private val converter = {
    val fns = schema.map { field =>
      val fn: RowConverter.Updater[(Array[UTF8String], Array[Byte])] = field match {
        case f if f.name == VariantSchemas.genotypesFieldName =>
          val gSchema = f.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
          val converter = makeGenotypeConverter(gSchema)
          (samplesAndBlock, r, i) => {
            val genotypes = new Array[Any](samplesAndBlock._1.length)
            var sampleIdx = 0
            while (sampleIdx < genotypes.length) {
              val sample = samplesAndBlock._1(sampleIdx)
              // Get the relevant 2 bits for the sample from the block
              // The i-th sample's call bits are the (i%4)-th pair within the (i/4)-th block
              val twoBits = samplesAndBlock._2(sampleIdx / 4) >> (2 * (sampleIdx % 4)) & 3
              genotypes(sampleIdx) = converter((sample, twoBits))
              sampleIdx += 1
            }
            r.update(i, new GenericArrayData(genotypes))
          }
        case f =>
          logger.info(
            s"Column $f cannot be derived from PLINK records. It will be null for each " +
            s"row."
          )
          (_, _, _) => ()
      }
      fn
    }
    new RowConverter[(Array[UTF8String], Array[Byte])](schema, fns.toArray)
  }

  private def makeGenotypeConverter(gSchema: StructType): RowConverter[(UTF8String, Int)] = {
    val functions = gSchema.map { field =>
      val fn: RowConverter.Updater[(UTF8String, Int)] = field match {
        case f if structFieldsEqualExceptNullability(f, VariantSchemas.sampleIdField) =>
          (sampleAndTwoBits, r, i) => {
            r.update(i, sampleAndTwoBits._1)
          }
        case f if structFieldsEqualExceptNullability(f, VariantSchemas.callsField) =>
          (sampleAndTwoBits, r, i) => r.update(i, twoBitsToCalls(sampleAndTwoBits._2))
        case f =>
          logger.info(
            s"Genotype field $f cannot be derived from PLINK files. It will be null " +
            s"for each sample."
          )
          (_, _, _) => ()
      }
      fn
    }
    new RowConverter[(UTF8String, Int)](gSchema, functions.toArray)
  }

  def convertRow(
      bimRow: InternalRow,
      sampleIds: Array[UTF8String],
      gtBlock: Array[Byte]): InternalRow = {
    converter((sampleIds, gtBlock), bimRow)
  }
}
