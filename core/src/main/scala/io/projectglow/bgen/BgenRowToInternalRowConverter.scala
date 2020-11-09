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

package io.projectglow.bgen

import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.common.{BgenGenotype, BgenRow, GlowLogging, VariantSchemas}
import io.projectglow.sql.expressions.HardCalls
import io.projectglow.sql.util.RowConverter

/**
 * Converts [[BgenRow]]s into [[InternalRow]] with a given required schema. During construction,
 * this class will throw an [[IllegalArgumentException]] if any of the fields in the required
 * schema cannot be derived from a BGEN record.
 */
class BgenRowToInternalRowConverter(schema: StructType, hardCallsThreshold: Double)
    extends GlowLogging {
  import io.projectglow.common.VariantSchemas._
  private val converter = {
    val fns = schema.map { field =>
      val fn: RowConverter.Updater[BgenRow] = field match {
        case f if structFieldsEqualExceptNullability(f, contigNameField) =>
          (bgen, r, i) => r.update(i, UTF8String.fromString(bgen.contigName))
        case f if structFieldsEqualExceptNullability(f, startField) =>
          (bgen, r, i) => r.setLong(i, bgen.start)
        case f if structFieldsEqualExceptNullability(f, endField) =>
          (bgen, r, i) => r.setLong(i, bgen.end)
        case f if structFieldsEqualExceptNullability(f, namesField) =>
          (bgen, r, i) => r.update(i, convertStringList(bgen.names))
        case f if structFieldsEqualExceptNullability(f, refAlleleField) =>
          (bgen, r, i) => r.update(i, UTF8String.fromString(bgen.referenceAllele))
        case f if structFieldsEqualExceptNullability(f, alternateAllelesField) =>
          (bgen, r, i) => r.update(i, convertStringList(bgen.alternateAlleles))
        case f if f.name == VariantSchemas.genotypesFieldName =>
          val gSchema = f.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
          val converter = makeGenotypeConverter(gSchema, hardCallsThreshold)
          (bgen, r, i) => {
            val genotypes = new Array[Any](bgen.genotypes.size)
            var j = 0
            while (j < genotypes.length) {
              val numAlleles = 1 + bgen.alternateAlleles.length
              genotypes(j) = converter((numAlleles, bgen.genotypes(j)))
              j += 1
            }
            r.update(i, new GenericArrayData(genotypes))
          }
        case f =>
          logger.info(
            s"Column $f cannot be derived from BGEN records. It will be null for each " +
            s"row."
          )
          (_, _, _) => ()
      }
      fn
    }
    new RowConverter[BgenRow](schema, fns.toArray)
  }

  private def makeGenotypeConverter(
      gSchema: StructType,
      hardCallsThreshold: Double): RowConverter[(Int, BgenGenotype)] = {
    val functions = gSchema.map { field =>
      val fn: RowConverter.Updater[(Int, BgenGenotype)] = field match {
        case f if structFieldsEqualExceptNullability(f, sampleIdField) =>
          (g, r, i) => {
            if (g._2.sampleId.isDefined) {
              r.update(i, UTF8String.fromString(g._2.sampleId.get))
            }
          }
        case f if structFieldsEqualExceptNullability(f, phasedField) =>
          (g, r, i) => g._2.phased.foreach(r.setBoolean(i, _))
        case f if structFieldsEqualExceptNullability(f, callsField) =>
          (g, r, i) =>
            if (g._2.phased.isDefined && g._2.ploidy == Some(2)) {
              val hardCalls = HardCalls.getHardCalls(
                hardCallsThreshold,
                g._1, // Number of alleles
                g._2.phased.get,
                g._2.posteriorProbabilities.length,
                g._2.posteriorProbabilities.apply
              )
              r.update(i, hardCalls)
            } else {
              // Set hard calls to missing for non-diploids
              g._2.ploidy.foreach { p =>
                r.update(i, new GenericArrayData(Array.fill(p)(-1)))
              }
            }
        case f if structFieldsEqualExceptNullability(f, ploidyField) =>
          (g, r, i) => g._2.ploidy.foreach(r.setInt(i, _))
        case f if structFieldsEqualExceptNullability(f, posteriorProbabilitiesField) =>
          (g, r, i) => r.update(i, new GenericArrayData(g._2.posteriorProbabilities))
        case f =>
          logger.info(
            s"Genotype field $f cannot be derived from BGEN genotypes. It will be null " +
            s"for each sample."
          )
          (_, _, _) => ()
      }
      fn
    }
    new RowConverter[(Int, BgenGenotype)](gSchema, functions.toArray)
  }

  def convertRow(bgenRow: BgenRow): InternalRow = converter(bgenRow)

  private def convertStringList(strings: Seq[String]): GenericArrayData = {
    var i = 0
    val out = new Array[Any](strings.size)
    while (i < strings.size) {
      out(i) = UTF8String.fromString(strings(i))
      i += 1
    }
    new GenericArrayData(out)
  }
}
