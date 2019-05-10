package com.databricks.bgen

import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.hls.common.HLSLogging
import com.databricks.hls.sql.util.RowConverter
import com.databricks.vcf.{BgenGenotype, BgenRow, VariantSchemas}

/**
 * Converts [[BgenRow]]s into [[InternalRow]] with a given required schema. During construction,
 * this class will throw an [[IllegalArgumentException]] if any of the fields in the required
 * schema cannot be derived from a BGEN record.
 */
class BgenRowConverter(schema: StructType) extends HLSLogging {
  import VariantSchemas._
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
          (bgen, r, i) => r.update(i, new GenericArrayData(convertStringList(bgen.names)))
        case f if structFieldsEqualExceptNullability(f, refAlleleField) =>
          (bgen, r, i) => r.update(i, UTF8String.fromString(bgen.referenceAllele))
        case f if structFieldsEqualExceptNullability(f, alternateAllelesField) =>
          (bgen, r, i) =>
            r.update(i, new GenericArrayData(convertStringList(bgen.alternateAlleles)))
        case f if f.name == VariantSchemas.genotypesFieldName =>
          val gSchema = f.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
          val converter = makeGenotypeConverter(gSchema)
          (bgen, r, i) => {
            val genotypes = new Array[InternalRow](bgen.genotypes.size)
            var j = 0
            while (j < genotypes.length) {
              genotypes(j) = converter(bgen.genotypes(j))
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

  private def makeGenotypeConverter(gSchema: StructType): RowConverter[BgenGenotype] = {
    val functions = gSchema.map { field =>
      val fn: RowConverter.Updater[BgenGenotype] = field match {
        case f if structFieldsEqualExceptNullability(f, sampleIdField) =>
          (g, r, i) => {
            if (g.sampleId.isDefined) {
              r.update(i, UTF8String.fromString(g.sampleId.get))
            }
          }
        case f if structFieldsEqualExceptNullability(f, posteriorProbabilitiesField) =>
          (g, r, i) => r.update(i, new GenericArrayData(g.posteriorProbabilities))
        case f =>
          logger.info(
            s"Genotype field $f cannot be derived from BGEN genotypes. It will be null " +
            s"for each sample."
          )
          (_, _, _) => ()
      }
      fn
    }
    new RowConverter[BgenGenotype](gSchema, functions.toArray, copy = true)
  }

  def convertRow(bgenRow: BgenRow): InternalRow = converter(bgenRow)

  private def convertStringList(strings: Seq[String]): Seq[UTF8String] = {
    var i = 0
    val out = new Array[UTF8String](strings.size)
    while (i < strings.size) {
      out(i) = UTF8String.fromString(strings(i))
      i += 1
    }
    out
  }
}
