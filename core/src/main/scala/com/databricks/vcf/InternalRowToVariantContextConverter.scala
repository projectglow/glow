package com.databricks.vcf

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext._
import htsjdk.variant.vcf.{VCFConstants, VCFHeader}
import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.hls.common.{HLSLogging, HasStringency}

/**
 * Converts internal rows with the provided schema into HTSJDK variant context.
 *
 * @param rowSchema The schema of the rows that will be converted by this instance. The schema
 *                  must match the provided rows exactly.
 * @param vcfHeader A VCF header used for validation. The header is only used to provide warnings
 *                  or validation errors if the fields in the data either don't exist in the header
 *                  or have incompatible types. These warnings/errors can be useful if downstream
 *                  calculations depend on certain fields being present.
 * @param stringency How seriously to treat validation errors vs the VCF header.
 */
class InternalRowToVariantContextConverter(
    rowSchema: StructType,
    vcfHeader: VCFHeader,
    val stringency: ValidationStringency)
    extends HLSLogging
    with HasStringency {
  import VariantSchemas._

  private val alleles = scala.collection.mutable.ArrayBuffer[Allele]()
  private val genotypeSchema = rowSchema
    .find(_.name == "genotypes")
    .map(_.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])
  val infoKeysParsedWithoutHeader = scala.collection.mutable.HashSet.empty[String]
  val formatKeysParsedWithoutHeader = scala.collection.mutable.HashSet.empty[String]

  def validate(): Unit = {
    rowSchema.filter(_.name.startsWith("INFO_")).foreach { f =>
      val headerLine = vcfHeader.getInfoHeaderLine(f.name.stripPrefix("INFO_"))
      if (headerLine == null) {
        provideWarning(s"Column ${f.name} does not have a matching VCF header line")
      } else if (f.dataType != VCFSchemaInferer.typeForHeader(headerLine)) {
        provideWarning(
          s"Column ${f.name} has a VCF header line with the same ID, but " +
          s"the types are not compatible. " +
          s"Header type: ${headerLine.getType},${headerLine.getCountType} SQL type: ${f.dataType}"
        )
      }
    }

    genotypeSchema.foreach { schema =>
      schema.filter(_.name != "GT").foreach { f =>
        val realName = GenotypeFields.reverseAliases.getOrElse(f.name, f.name)
        val headerLine = vcfHeader.getFormatHeaderLine(realName)
        if (headerLine == null) {
          provideWarning(
            s"Genotype field ${f.name} does not have a matching VCF header " +
            s"line"
          )
        } else if (f.dataType != VCFSchemaInferer.typeForHeader(headerLine)) {
          provideWarning(
            s"Genotype field ${f.name} has a VCF header line with the " +
            s"same ID, but the types are not compatible. " +
            s"Header type: ${headerLine.getType},${headerLine.getCountType} " +
            s"SQL type: ${f.dataType}"
          )
        }
      }
    }
  }

  private lazy val converter: InternalRow => Option[VariantContext] = {
    val fns = rowSchema.map { field =>
      val fn: (VariantContextBuilder, InternalRow, Int) => VariantContextBuilder = field match {
        case f if structFieldsEqualExceptNullability(f, contigNameField) => updateContigName
        case f if structFieldsEqualExceptNullability(f, startField) => updateStart
        case f if structFieldsEqualExceptNullability(f, endField) => updateEnd
        case f if structFieldsEqualExceptNullability(f, namesField) => updateNames
        case f if structFieldsEqualExceptNullability(f, refAlleleField) => updateReferenceAllele
        case f if structFieldsEqualExceptNullability(f, alternateAllelesField) => updateAltAlleles
        case f if structFieldsEqualExceptNullability(f, qualField) => updateQual
        case f if structFieldsEqualExceptNullability(f, filtersField) => updateFilters
        case f if structFieldsEqualExceptNullability(f, attributesField) => updateAttributes
        case f if f.name.startsWith(infoFieldPrefix) =>
          (vc, row, i) => updateInfoField(f, vc, row, i)
        case f if f.name == genotypesFieldName =>
          // Explicitly do nothing for now since we need to wait until we parse alleles to parse
          // genotypes
          (vc, _, _) => vc
        case f =>
          logger.info(
            s"Field $f is present in data schema but does not have a " +
            s"VCF representation"
          )
          (vc, _, _) => vc
      }
      fn
    }.toArray

    val genotypeIdx = rowSchema.indexWhere(_.name == genotypesFieldName)
    val genotypeBuilder = if (genotypeIdx > -1) {
      makeGenotypeConverter(genotypeSchema.get)
    } else {
      null
    }

    row => {
      val builder = new VariantContextBuilder()
      builder.source("Unknown")
      var i = 0
      while (i < fns.length) {
        if (!row.isNullAt(i)) {
          fns(i)(builder, row, i)
        }
        i += 1
      }

      // Now that we've collected all alleles, add them to the variant context builder
      builder.alleles(alleles.asJava)

      if (genotypeIdx > -1) {
        builder.genotypes(genotypeBuilder(row.getArray(genotypeIdx)))
      }

      try {
        Option(builder.make())
      } catch {
        case NonFatal(ex) =>
          provideWarning(s"Could not build variant context: ${ex.getMessage}")
          None
      }
    }
  }

  private def makeGenotypeConverter(gSchema: StructType): ArrayData => GenotypesContext = {
    val fns = gSchema.map { field =>
      val fn: (GenotypeBuilder, InternalRow, Int) => GenotypeBuilder = field match {
        case f if structFieldsEqualExceptNullability(f, sampleIdField) => updateSampleId
        case f if structFieldsEqualExceptNullability(f, genotypeField) => updateGT
        case f if structFieldsEqualExceptNullability(f, depthField) => updateDP
        case f if structFieldsEqualExceptNullability(f, genotypeFiltersField) => updateGTFilters
        case f if structFieldsEqualExceptNullability(f, genotypeLikelihoodsField) => updateGL
        case f if structFieldsEqualExceptNullability(f, conditionalQualityField) => updateGQ
        case f if structFieldsEqualExceptNullability(f, phredLikelihoodsField) => updatePL
        case f if structFieldsEqualExceptNullability(f, alleleDepthsField) => updateAD
        case f if structFieldsEqualExceptNullability(f, otherFieldsField) => updateOtherFields
        case f => (gb, row, i) => updateFormatField(f, gb, row, i)
      }
      fn
    }

    array => {
      val ctx = GenotypesContext.create(array.numElements())
      var i = 0
      while (i < array.numElements()) {
        var j = 0
        val builder = new GenotypeBuilder("")
        val row = array.getStruct(i, gSchema.length)
        while (j < fns.length) {
          if (!row.isNullAt(j)) {
            fns(j)(builder, row, j)
          }

          j += 1
        }
        ctx.add(builder.make())
        i += 1
      }
      ctx
    }
  }

  def convert(row: InternalRow): Option[VariantContext] = {
    alleles.clear()
    converter(row)
  }

  private def updateContigName(
      vc: VariantContextBuilder,
      row: InternalRow,
      offset: Int): VariantContextBuilder = {
    vc.chr(row.getString(offset))
  }

  private def updateStart(
      vc: VariantContextBuilder,
      row: InternalRow,
      offset: Int): VariantContextBuilder = {
    vc.start(row.getLong(offset) + 1)
  }

  private def updateEnd(
      vc: VariantContextBuilder,
      row: InternalRow,
      offset: Int): VariantContextBuilder = {
    vc.stop(row.getLong(offset))
  }

  private def updateQual(
      vc: VariantContextBuilder,
      row: InternalRow,
      offset: Int): VariantContextBuilder = {
    vc.log10PError(row.getDouble(offset) / -10)
  }

  private def updateNames(
      vc: VariantContextBuilder,
      row: InternalRow,
      offset: Int): VariantContextBuilder = {
    val names = arrayDataToStringList(row.getArray(offset))
    val str = if (names.isEmpty) {
      VCFConstants.EMPTY_ID_FIELD
    } else {
      names.mkString(VCFConstants.ID_FIELD_SEPARATOR)
    }
    vc.id(str)
  }

  // We can't update the alleles in the variant context until we've seen both the reference
  // and alternate alleles. So we do a fake update and simply push the alleles to buffer.
  private def updateReferenceAllele(
      vc: VariantContextBuilder,
      row: InternalRow,
      offset: Int): VariantContextBuilder = {
    // Note: The reference allele is always prepended so that it is guaranteed to be at the front
    // of the allele list
    alleles.prepend(Allele.create(row.getString(offset).getBytes, true))
    vc
  }

  private def updateAltAlleles(
      vc: VariantContextBuilder,
      row: InternalRow,
      offset: Int): VariantContextBuilder = {
    val alts = arrayDataToStringList(row.getArray(offset))
    alleles.appendAll(alts.map(a => Allele.create(a.getBytes, false)))
    vc
  }

  private def updateFilters(
      vc: VariantContextBuilder,
      row: InternalRow,
      offset: Int): VariantContextBuilder = {
    val filters = arrayDataToStringList(row.getArray(offset))
    if (filters.isEmpty) {
      vc.unfiltered()
    } else if (filters == Seq(VCFConstants.PASSES_FILTERS_v4)) {
      vc.passFilters()
    } else {
      vc.filters(filters: _*)
    }
  }

  private def updateAttributes(
      vc: VariantContextBuilder,
      row: InternalRow,
      offset: Int): VariantContextBuilder = {
    val attrs = row.getMap(offset)
    val keys = arrayDataToStringList(attrs.keyArray())
    val values = arrayDataToStringList(attrs.valueArray())
    var i = 0
    while (i < keys.size) {
      val headerLine = vcfHeader.getFormatHeaderLine(keys(i))
      if (headerLine == null && !infoKeysParsedWithoutHeader.contains(keys(i))) {
        provideWarning(s"INFO field ${keys(i)} does not have a matching VCF header line.")
        infoKeysParsedWithoutHeader.add(keys(i))
      }
      vc.attribute(keys(i), if (values(i).nonEmpty) values(i) else VCFConstants.MISSING_VALUE_v4)
      i += 1
    }
    vc
  }

  private def updateInfoField(
      field: StructField,
      vc: VariantContextBuilder,
      row: InternalRow,
      offset: Int): VariantContextBuilder = {
    val realName = field.name.stripPrefix("INFO_")
    vc.attribute(realName, fieldToString(field, row, offset))
  }

  private def updateSampleId(
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    genotype.name(row.getString(offset))
  }

  private def updateGT(
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    val struct = row.getStruct(offset, Genotype.schema.length)
    val calls = struct
      .getArray(0)
      .toIntArray()
      .map { idx =>
        if (idx == -1) {
          Allele.NO_CALL
        } else {
          // Will throw out of bounds exception if there's no allele
          alleles(idx)
        }
      }
      .toSeq
    val phased = struct.getBoolean(1)
    genotype.phased(phased)
    genotype.alleles(calls.asJava)
    genotype
  }

  private def updateDP(
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    genotype.DP(row.getInt(offset))
  }

  private def updateGTFilters(
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    val filters = arrayDataToStringList(row.getArray(offset))
    genotype.filter(filters.mkString(VCFConstants.FILTER_CODE_SEPARATOR))
  }

  private def updatePL(
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    genotype.PL(row.getArray(offset).toIntArray())
    genotype
  }

  private def updateGL(
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    genotype.PL(row.getArray(offset).toDoubleArray())
    genotype
  }

  private def updateAD(
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    genotype.AD(row.getArray(offset).toIntArray())
  }

  private def updateGQ(
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    genotype.GQ(row.getInt(offset))
  }

  private def updateOtherFields(
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    val attrs = row.getMap(offset)
    val keys = arrayDataToStringList(attrs.keyArray())
    val values = arrayDataToStringList(attrs.valueArray())
    var i = 0
    while (i < keys.size) {
      val headerLine = vcfHeader.getInfoHeaderLine(keys(i))
      if (headerLine == null && !formatKeysParsedWithoutHeader.contains(keys(i))) {
        provideWarning(
          s"Genotype field ${keys(i)} does not have a matching " +
          s"VCF header line"
        )
        formatKeysParsedWithoutHeader.add(keys(i))
      }
      genotype.attribute(keys(i), values(i))
      i += 1
    }
    genotype
  }

  private def updateFormatField(
      field: StructField,
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    val realName = GenotypeFields.reverseAliases.getOrElse(field.name, field.name)
    val headerLine = vcfHeader.getFormatHeaderLine(realName)
    if (headerLine == null && !formatKeysParsedWithoutHeader.contains(field.name)) {
      provideWarning(
        s"Genotype field ${field.name} does not have a matching " +
        s"VCF header line"
      )
      formatKeysParsedWithoutHeader.add(field.name)
    }
    genotype.attribute(realName, fieldToString(field, row, offset))
  }

  private def fieldToString(field: StructField, row: InternalRow, offset: Int): String = {
    val valueToConvert = field.dataType match {
      case dt: ArrayType =>
        row.getArray(offset).toObjectArray(dt.elementType)
      case dt =>
        row.get(offset, dt)
    }
    val base = VariantContextToVCFRowConverter.parseObjectAsString(valueToConvert)
    if (base.isEmpty) {
      // Missing values are represented by '.' instead of empty strings
      VCFConstants.MISSING_VALUE_v4
    } else {
      base
    }
  }

  private def arrayDataToStringList(array: ArrayData): Seq[String] = {
    array.toObjectArray(StringType).map(_.asInstanceOf[UTF8String].toString)
  }
}
