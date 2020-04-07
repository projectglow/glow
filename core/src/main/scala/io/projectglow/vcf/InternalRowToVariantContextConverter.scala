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

package io.projectglow.vcf

import java.util.{ArrayList => JArrayList, Arrays => JArrays}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext._
import htsjdk.variant.vcf.{VCFConstants, VCFHeader, VCFHeaderLine}
import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

import io.projectglow.common.{GenotypeFields, GlowLogging, HasStringency, VariantSchemas}

/**
 * Converts internal rows with the provided schema into HTSJDK variant context.
 *
 * @param rowSchema     The schema of the rows that will be converted by this instance. The schema
 *                      must match the provided rows exactly.
 * @param headerLineSet VCF header lines used for validation. The header is only used to provide
 *                      warnings or validation errors if the fields in the data either don't exist
 *                      in the header or have incompatible types. These warnings/errors can be
 *                      useful if downstream calculations depend on certain fields being present.
 * @param stringency    How seriously to treat validation errors vs the VCF header.
 */
class InternalRowToVariantContextConverter(
    rowSchema: StructType,
    headerLineSet: Set[VCFHeaderLine],
    val stringency: ValidationStringency)
    extends GlowLogging
    with HasStringency
    with Serializable {
  import io.projectglow.common.ConverterUtils._
  import io.projectglow.common.VariantSchemas._

  private val alleles = scala.collection.mutable.ArrayBuffer[Allele]()
  private val genotypeSchema = InternalRowToVariantContextConverter.getGenotypeSchema(rowSchema)
  private val infoKeysParsedWithoutHeader = scala.collection.mutable.HashSet.empty[String]
  private val formatKeysParsedWithoutHeader = scala.collection.mutable.HashSet.empty[String]

  private val vcFieldsToIgnore =
    Set(VariantSchemas.splitFromMultiAllelicField.name, VariantSchemas.genotypesFieldName)

  // Genotype field names that are expected to not have headers
  private val genotypeFieldsWithoutHeaders = Set("sampleId", "otherFields")

  // Sample-free VCF header
  val vcfHeader: VCFHeader = new VCFHeader(headerLineSet.asJava)

  def validate(): Unit = {
    rowSchema.filter(_.name.startsWith(infoFieldPrefix)).foreach { f =>
      val headerLine =
        vcfHeader.getInfoHeaderLine(f.name.stripPrefix(infoFieldPrefix))
      if (headerLine == null) {
        provideWarning(s"Column ${f.name} does not have a matching VCF header line")
      } else if (!VCFSchemaInferrer
          .typesForHeader(headerLine)
          .exists(t => SQLUtils.dataTypesEqualExceptNullability(t, f.dataType))) {
        provideWarning(
          s"Column ${f.name} has a VCF header line with the same ID, but " +
          s"the types are not compatible. " +
          s"Header type: ${headerLine.getType},${headerLine.getCountType} SQL type: ${f.dataType}"
        )
      }
    }

    genotypeSchema.foreach { schema =>
      schema.filter(f => !genotypeFieldsWithoutHeaders.contains(f.name)).foreach { f =>
        val realName = GenotypeFields.reverseAliases.getOrElse(f.name, f.name)
        val headerLine = vcfHeader.getFormatHeaderLine(realName)
        if (headerLine == null) {
          provideWarning(
            s"Genotype field ${f.name} does not have a matching VCF header " +
            s"line"
          )
        } else if (!VCFSchemaInferrer
            .typesForHeader(headerLine)
            .exists(t => SQLUtils.dataTypesEqualExceptNullability(t, f.dataType))) {
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
    val vcFields = rowSchema.fields
    val fns =
      new Array[(VariantContextBuilder, InternalRow, Int) => VariantContextBuilder](vcFields.length)
    var idx = 0
    while (idx < vcFields.length) {
      fns(idx) = vcFields(idx) match {
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
        case f if vcFieldsToIgnore.contains(f.name) =>
          // Explicitly do nothing for fields that are added by our VCF reader
          (vc, _, _) => vc
        case f =>
          logger.info(
            s"Field $f is present in data schema but does not have a " +
            s"VCF representation"
          )
          (vc, _, _) => vc
      }
      idx += 1
    }

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
    val gtFields = gSchema.fields
    val fns = new Array[(GenotypeBuilder, InternalRow, Int) => GenotypeBuilder](gtFields.length)
    var idx = 0
    while (idx < gtFields.length) {
      fns(idx) = gtFields(idx) match {
        case f if structFieldsEqualExceptNullability(f, sampleIdField) => updateSampleId
        case f if structFieldsEqualExceptNullability(f, phasedField) => updateGTPhased
        case f if structFieldsEqualExceptNullability(f, callsField) => updateGTCalls
        case f if structFieldsEqualExceptNullability(f, depthField) => updateDP
        case f if structFieldsEqualExceptNullability(f, genotypeFiltersField) => updateGTFilters
        case f if structFieldsEqualExceptNullability(f, genotypeLikelihoodsField) => updateGL
        case f if structFieldsEqualExceptNullability(f, conditionalQualityField) => updateGQ
        case f if structFieldsEqualExceptNullability(f, phredLikelihoodsField) => updatePL
        case f if structFieldsEqualExceptNullability(f, alleleDepthsField) => updateAD
        case f if structFieldsEqualExceptNullability(f, otherFieldsField) => updateOtherFields
        case f => (gb, row, i) => updateFormatField(f, gb, row, i)
      }
      idx += 1
    }

    array => {
      val ctx = GenotypesContext.create(array.numElements())
      var i = 0
      while (i < array.numElements()) {
        var j = 0
        val builder = new GenotypeBuilder("")
        val row = array.getStruct(i, gtFields.length)
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

  private def createAnnotationArray(schema: StructType, effects: ArrayData): Array[String] = {
    val annotations = new Array[String](effects.numElements)
    var i = 0
    while (i < annotations.length) {
      val strBuilder = new StringBuilder()
      val effect = effects.getStruct(i, schema.size)
      var j = 0
      while (j < schema.size) {
        if (!effect.isNullAt(j)) {
          val strEffect = schema.fields(j).dataType match {
            case ArrayType(StringType, _) => // &-delimited list
              effect
                .getArray(j)
                .toObjectArray(StringType)
                .mkString(AnnotationUtils.arrayDelimiter)
            case st if st.isInstanceOf[StructType] => // /-delimited pair
              val schema = st.asInstanceOf[StructType]
              val arr = schema.fields.head.dataType match {
                case IntegerType =>
                  effect
                    .getStruct(j, schema.size)
                    .toSeq(schema)
                    .filterNot(_ == null) // Ratio denominator is optional
                case StringType =>
                  effect
                    .getStruct(j, schema.size)
                    .toSeq(schema)
                    .filterNot(_ == null) // Second value may be missing
              }
              arr.mkString(AnnotationUtils.structDelimiter)
            case IntegerType => effect.getInt(j)
            case StringType => effect.getUTF8String(j)
          }
          strBuilder.append(strEffect)
        }
        if (j < schema.size - 1) {
          strBuilder.append(AnnotationUtils.annotationDelimiter)
        }
        j += 1
      }
      annotations(i) = strBuilder.toString
      i += 1
    }
    annotations
  }

  private def updateInfoField(
      field: StructField,
      vc: VariantContextBuilder,
      row: InternalRow,
      offset: Int): VariantContextBuilder = {
    val realName = field.name.stripPrefix(infoFieldPrefix)

    // We generate the info fields in the vc with the same type they have in InternalRow for
    // correct reciprocal conversion between vc and InternalRow using
    // InternalRowToVariantContextConverter and VariantContextToInternalRowConverter. When using
    // in writing to file these fields are converted to strings in VCFStreamWriter.
    vc.attribute(
      realName,
      field.dataType match {
        case dt: ArrayType if dt.elementType.isInstanceOf[StructType] =>
          // Annotation (eg. CSQ, ANN)
          createAnnotationArray(dt.elementType.asInstanceOf[StructType], row.getArray(offset))
        case _ => parseField(field, row, offset)
      }
    )
  }

  private def updateSampleId(
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    genotype.name(row.getString(offset))
  }

  private def updateGTPhased(
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    val phased = row.getBoolean(offset)
    genotype.phased(phased)
    genotype
  }

  private def updateGTCalls(
      genotype: GenotypeBuilder,
      row: InternalRow,
      offset: Int): GenotypeBuilder = {
    val calls = row
      .getArray(offset)
      .toIntArray()
    val gtAlleles = new JArrayList[Allele](calls.length)
    var callIdx = 0
    while (callIdx < calls.length) {
      val alleleIdx = calls(callIdx)
      val allele = if (alleleIdx == -1) {
        Allele.NO_CALL
      } else {
        // Will throw out of bounds exception if there's no allele
        alleles(alleleIdx)
      }
      gtAlleles.add(allele)
      callIdx += 1
    }
    genotype.alleles(gtAlleles)
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
    genotype.attribute(realName, parseField(field, row, offset))
  }

  private def parseField(field: StructField, row: InternalRow, offset: Int): AnyRef = {
    val value = field.dataType match {
      case StringType =>
        row.getString(offset)
      case ArrayType(StringType, _) =>
        val arrayData = row.getArray(offset)
        val arr = new JArrayList[String](arrayData.numElements())
        var i = 0
        while (i < arrayData.numElements()) {
          arr.add(arrayData.getUTF8String(i).toString)
          i += 1
        }
        arr
      case dt: ArrayType =>
        new JArrayList(JArrays.asList(row.getArray(offset).toObjectArray(dt.elementType): _*))
      case dt =>
        row.get(offset, dt)
    }

    value match {
      case null => VCFConstants.MISSING_VALUE_v4
      case "" => VCFConstants.MISSING_VALUE_v4
      case _ => value
    }
  }
}

object InternalRowToVariantContextConverter {
  def getGenotypeSchema(rowSchema: StructType): Option[StructType] = {
    try {
      rowSchema
        .find(_.name == VariantSchemas.genotypesFieldName)
        .map(_.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])
    } catch {
      case e: ClassCastException =>
        throw new IllegalArgumentException(
          "`genotypes` column must be an array of structs: " + e.getMessage)
    }
  }
}
