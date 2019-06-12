package com.databricks.bgen

import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, StructType}

import com.databricks.hls.common.HLSLogging
import com.databricks.vcf.{BgenGenotype, BgenRow, ConverterUtils, VariantSchemas}

/**
 * Converts internal rows to BGEN rows. Includes logic to infer phasing and ploidy if missing (eg.
 * when converting from VCF rows with no GT field), using the number of alleles and the number of
 * posterior probabilities.
 *
 * - If phasing and ploidy are missing, we assume ploidy is defaultPloidy.
 * - If phasing is missing:
 *   - If no posterior probabilities are present, we assume phasing is defaultPhasing.
 *   - If the number of posterior probabilities matches the case that the probability represents:
 *      - Either phased or unphased data: we assume phasing is defaultPhasing.
 *      - Phased data: we assume the data is phased.
 *      - Unphased data: we assume the data is unphased.
 *      - Neither: we throw an exception
 * - If ploidy is missing:
 *   - If no posterior probabilities are present, we assume ploidy is defaultPloidy.
 *   - If phased, we try to calculate the ploidy directly.
 *   - If unphased, we try to find the ploidy between [1, maxPloidy].
 *
 * @throws IllegalStateException if phasing or ploidy cannot be inferred or a single row contains
 *                               both unphased and phased data.
 */
class InternalRowToBgenRowConverter(
    rowSchema: StructType,
    maxPloidy: Int,
    defaultPloidy: Int,
    defaultPhasing: Boolean)
    extends HLSLogging {
  import VariantSchemas._
  import ConverterUtils._

  private val genotypeSchema = rowSchema
    .find(_.name == "genotypes")
    .map(_.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])

  def convert(row: InternalRow): BgenRow = {
    val fns = rowSchema.map { field =>
      val fn: (BgenRow, InternalRow, Int) => BgenRow = field match {
        case f if structFieldsEqualExceptNullability(f, contigNameField) =>
          (bgen, r, i) => bgen.copy(contigName = r.getString(i))
        case f if structFieldsEqualExceptNullability(f, startField) =>
          (bgen, r, i) => bgen.copy(start = r.getLong(i))
        case f if structFieldsEqualExceptNullability(f, endField) =>
          (bgen, r, i) => bgen.copy(end = r.getLong(i))
        case f if structFieldsEqualExceptNullability(f, namesField) =>
          (bgen, r, i) => bgen.copy(names = arrayDataToStringList(r.getArray(i)))
        case f if structFieldsEqualExceptNullability(f, refAlleleField) =>
          (bgen, r, i) => bgen.copy(referenceAllele = r.getString(i))
        case f if structFieldsEqualExceptNullability(f, alternateAllelesField) =>
          (bgen, r, i) => bgen.copy(alternateAlleles = arrayDataToStringList(r.getArray(i)))
        case f if f.name == VariantSchemas.genotypesFieldName =>
          // Explicitly do nothing for now since we need to wait until we parse alleles to parse
          // genotypes
          (bgen, _, _) => bgen
        case _ => (bgen, _, _) => bgen
      }
      fn
    }.toArray

    val genotypeIdx = rowSchema.indexWhere(_.name == genotypesFieldName)
    val genotypeConverter = if (genotypeIdx > -1) {
      makeGenotypeConverter(genotypeSchema.get)
    } else {
      null
    }

    var bgenRow = BgenRow(
      contigName = "",
      start = -1,
      end = -1,
      names = Seq.empty,
      referenceAllele = "",
      alternateAlleles = Seq.empty,
      genotypes = Seq.empty
    )
    var i = 0
    while (i < fns.length) {
      if (!row.isNullAt(i)) {
        bgenRow = fns(i)(bgenRow, row, i)
      }
      i += 1
    }

    if (genotypeIdx > -1) {
      val numAlleles = 1 + bgenRow.alternateAlleles.length
      bgenRow = bgenRow.copy(genotypes = genotypeConverter(numAlleles, row.getArray(genotypeIdx)))
    }

    bgenRow
  }

  private def makeGenotypeConverter(gSchema: StructType): (Int, ArrayData) => Seq[BgenGenotype] = {
    val fns = gSchema.map { field =>
      val fn: (BgenGenotype, InternalRow, Int) => BgenGenotype = field match {
        case f if structFieldsEqualExceptNullability(f, sampleIdField) =>
          (g, r, i) => g.copy(sampleId = Some(r.getString(i)))
        case f if structFieldsEqualExceptNullability(f, phasedField) =>
          (g, r, i) => g.copy(phased = Some(r.getBoolean(i)))
        case f if structFieldsEqualExceptNullability(f, callsField) =>
          (g, r, i) => g.copy(ploidy = Some(r.getArray(i).numElements))
        case f if structFieldsEqualExceptNullability(f, ploidyField) =>
          (g, r, i) => g.copy(ploidy = Some(r.getInt(i)))
        case f if structFieldsEqualExceptNullability(f, posteriorProbabilitiesField) =>
          (g, r, i) => g.copy(posteriorProbabilities = r.getArray(i).toDoubleArray)
        case _ => (g, _, _) => g
      }
      fn
    }

    (numAlleles, array) => {
      val gtArray = new Array[BgenGenotype](array.numElements)
      var rowPhasing: Option[Boolean] = None
      var i = 0
      while (i < array.numElements) {
        var j = 0
        var gt = BgenGenotype(
          sampleId = None,
          phased = None,
          ploidy = None,
          posteriorProbabilities = Seq.empty
        )
        val row = array.getStruct(i, gSchema.length)
        while (j < fns.length) {
          if (!row.isNullAt(j)) {
            gt = fns(j)(gt, row, j)
          }
          j += 1
        }

        // If neither phasing information nor ploidy is provided, use the default ploidy
        if (gt.phased.isEmpty && gt.ploidy.isEmpty) {
          gt = gt.copy(ploidy = Some(defaultPloidy))
        }

        // Infer phasing based on ploidy
        if (gt.phased.isEmpty) {
          val inferredPhasing = if (gt.posteriorProbabilities.isEmpty) {
            defaultPhasing
          } else {
            val numAllelesHaplotypes = gt.ploidy.get * numAlleles
            val numGenotypes = BgenConverterUtils.getNumGenotypes(gt.ploidy.get, numAlleles)
            val couldBePhased = gt.posteriorProbabilities.length == numAllelesHaplotypes
            val couldBeUnphased = gt.posteriorProbabilities.length == numGenotypes

            (couldBePhased, couldBeUnphased) match {
              case (true, true) => defaultPhasing
              case (true, false) => true
              case (false, true) => false
              case (false, false) =>
                throw new IllegalStateException(
                  "Phasing information cannot be inferred: " +
                  s"ploidy ${gt.ploidy.get}, $numAlleles alleles, " +
                  s"${gt.posteriorProbabilities.length} probabilities."
                )
            }
          }
          gt = gt.copy(phased = Some(inferredPhasing))
        }
        if (rowPhasing.isEmpty) {
          rowPhasing = gt.phased
        } else if (rowPhasing != gt.phased) {
          throw new IllegalStateException("Cannot have mixed phases in a single row.")
        }

        // Infer ploidy based on phasing information
        if (gt.ploidy.isEmpty) {
          val inferredPloidy = if (gt.posteriorProbabilities.isEmpty) {
            defaultPloidy
          } else if (gt.phased.get) {
            if (gt.posteriorProbabilities.length % numAlleles != 0) {
              throw new IllegalStateException(
                s"Ploidy cannot be inferred: phased, $numAlleles alleles, " +
                s"${gt.posteriorProbabilities.length} probabilities."
              )
            }
            gt.posteriorProbabilities.length / numAlleles
          } else {
            BgenConverterUtils.getPloidy(gt.posteriorProbabilities.length, numAlleles, maxPloidy)
          }
          gt = gt.copy(ploidy = Some(inferredPloidy))
        }

        gtArray(i) = gt
        i += 1
      }
      gtArray
    }
  }
}
