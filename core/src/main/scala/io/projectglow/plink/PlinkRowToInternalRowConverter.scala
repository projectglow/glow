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

  private val converter = {
    val fns = schema.map { field =>
      val fn: RowConverter.Updater[(Array[String], Array[Array[Int]])] = field match {
        case f if f.name == VariantSchemas.genotypesFieldName =>
          val gSchema = f.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
          val converter = makeGenotypeConverter(gSchema)
          (samplesAndCalls, r, i) => {
            val genotypes = new Array[Any](samplesAndCalls._1.length)
            var j = 0
            while (j < genotypes.length) {
              genotypes(j) = converter((samplesAndCalls._1(j), samplesAndCalls._2(j)))
              j += 1
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
    new RowConverter[(Array[String], Array[Array[Int]])](schema, fns.toArray)
  }

  private def makeGenotypeConverter(gSchema: StructType): RowConverter[(String, Array[Int])] = {
    val functions = gSchema.map { field =>
      val fn: RowConverter.Updater[(String, Array[Int])] = field match {
        case f if structFieldsEqualExceptNullability(f, VariantSchemas.sampleIdField) =>
          (samplesAndCalls, r, i) => {
            r.update(i, UTF8String.fromString(samplesAndCalls._1))
          }
        case f if structFieldsEqualExceptNullability(f, VariantSchemas.callsField) =>
          (samplesAndCalls, r, i) => r.update(i, new GenericArrayData(samplesAndCalls._2))
        case f =>
          logger.info(
            s"Genotype field $f cannot be derived from PLINK files. It will be null " +
            s"for each sample."
          )
          (_, _, _) => ()
      }
      fn
    }
    new RowConverter[(String, Array[Int])](gSchema, functions.toArray)
  }

  def convertRow(
      bimRow: InternalRow,
      sampleIds: Array[String],
      calls: Array[Array[Int]]): InternalRow = {
    converter((sampleIds, calls), bimRow)
  }
}
