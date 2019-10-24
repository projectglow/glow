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
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.common.{BgenGenotype, BgenRow, GlowLogging, VariantSchemas}
import io.projectglow.sql.util.RowConverter

/**
 * Creates an [[InternalRow]] with a given required schema. During construction,
 * this class will throw an [[IllegalArgumentException]] if any of the fields in the required
 * schema cannot be derived from a PLINK record.
 */
class PlinkRowToInternalRowConverter(schema: StructType) extends GlowLogging {
  import io.projectglow.common.VariantSchemas._
  import BimRow._

  private val converter = {
    val fns = schema.map { field =>
      val fn: RowConverter.Updater[(InternalRow, Array[String], Array[Array[Int]])] = field match {
        case f if structFieldsEqualExceptNullability(f, contigNameField) =>
          (bsc, r, i) => r.update(i, getContigName(bsc._1))
        case f if structFieldsEqualExceptNullability(f, namesField) =>
          (bsc, r, i) => r.update(i, getNames(bsc._1))
        case f if structFieldsEqualExceptNullability(f, positionField) =>
          (bsc, r, i) => r.update(i, getPosition(bsc._1))
        case f if structFieldsEqualExceptNullability(f, startField) =>
          (bsc, r, i) => r.update(i, getStart(bsc._1))
        case f if structFieldsEqualExceptNullability(f, endField) =>
          (bsc, r, i) => r.update(i, getStart(bsc._1) + getRefAllele(bsc._1).numChars)
        case f if structFieldsEqualExceptNullability(f, refAlleleField) =>
          (bsc, r, i) => r.update(i, getRefAllele(bsc._1))
        case f if structFieldsEqualExceptNullability(f, alternateAllelesField) =>
          (bsc, r, i) => r.update(i, getAlternateAlleles(bsc._1))
        case f if f.name == VariantSchemas.genotypesFieldName =>
          val gSchema = f.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
          val converter = makeGenotypeConverter(gSchema)
          (bsc, r, i) => {
            val genotypes = new Array[Any](bsc._2.length)
            var j = 0
            while (j < genotypes.length) {
              genotypes(j) = converter(bsc._2(j), bsc._3(j))
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
    new RowConverter[(InternalRow, Array[String], Array[Array[Int]])](schema, fns.toArray)
  }

  private def makeGenotypeConverter(gSchema: StructType): RowConverter[(String, Array[Int])] = {
    val functions = gSchema.map { field =>
      val fn: RowConverter.Updater[(String, Array[Int])] = field match {
        case f if structFieldsEqualExceptNullability(f, sampleIdField) =>
          (sc, r, i) => {
            r.update(i, UTF8String.fromString(sc._1))
          }
        case f if structFieldsEqualExceptNullability(f, callsField) =>
          (sc, r, i) => r.update(i, new GenericArrayData(sc._2))
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
      calls: Array[Array[Int]]): InternalRow = converter((bimRow, sampleIds, calls))
}

object BimRow {
  import VariantSchemas._

  def getContigName(row: InternalRow): UTF8String = {
    row.getUTF8String(bimSchema.fieldIndex(contigNameField.name))
  }

  def getNames(row: InternalRow): ArrayData = {
    row.getArray(bimSchema.fieldIndex(namesField.name))
  }

  def getPosition(row: InternalRow): Double = {
    row.getDouble(bimSchema.fieldIndex(positionField.name))
  }

  def getStart(row: InternalRow): Long = {
    row.getLong(bimSchema.fieldIndex(startField.name) - 1)
  }

  def getAlternateAlleles(row: InternalRow): ArrayData = {
    row.getArray(bimSchema.fieldIndex(alternateAllelesField.name))
  }

  def getRefAllele(row: InternalRow): UTF8String = {
    row.getUTF8String(bimSchema.fieldIndex(refAlleleField.name))
  }
}
