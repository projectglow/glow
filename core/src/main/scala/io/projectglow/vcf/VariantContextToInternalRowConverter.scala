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

import java.util.{HashMap => JHashMap, HashSet => JHashSet, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.{Allele, VariantContext, Genotype => HTSJDKGenotype}
import htsjdk.variant.vcf.{VCFConstants, VCFHeader}
import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.common.{GenotypeFields, GlowLogging, HasStringency, VariantSchemas}
import io.projectglow.sql.util.RowConverter

/**
 * Converts an HTSJDK variant context into a SparkSQL row with the provided schema.
 *
 * @param header The VCF header for this dataset
 * @param schema The actual for the emitted rows
 * @param stringency How strictly to validate input variant contexts. If strict, incorrect rows
 *                   will result in an exception. If lenient, we'll do a best effort to parse
 *                   the row and log warnings. If silent, best effort and continue.
 */
class VariantContextToInternalRowConverter(
    header: VCFHeader,
    schema: StructType,
    val stringency: ValidationStringency,
    writeSampleIds: Boolean = true)
    extends GlowLogging
    with HasStringency
    with Serializable {

  import io.projectglow.common.VariantSchemas._

  private val infoKeysParsedWithoutHeader = new JHashSet[String]()
  private val formatKeysParsedWithoutHeader = new JHashSet[String]()

  private def makeConverter(forSplit: Boolean) = {
    val fns = new Array[RowConverter.Updater[VariantContext]](schema.length)
    var i = 0
    while (i < fns.length) {
      val field = schema(i)
      fns(i) = field match {
        case f if structFieldsEqualExceptNullability(f, contigNameField) => updateContigName
        case f if structFieldsEqualExceptNullability(f, startField) => updateStart
        case f if structFieldsEqualExceptNullability(f, endField) => updateEnd
        case f if structFieldsEqualExceptNullability(f, namesField) => updateNames
        case f if structFieldsEqualExceptNullability(f, refAlleleField) => updateReferenceAllele
        case f if structFieldsEqualExceptNullability(f, alternateAllelesField) => updateAltAlleles
        case f if structFieldsEqualExceptNullability(f, qualField) => updateQual
        case f if structFieldsEqualExceptNullability(f, filtersField) => updateFilters
        case f if structFieldsEqualExceptNullability(f, attributesField) => updateAttributes
        case f if structFieldsEqualExceptNullability(f, splitFromMultiAllelicField) =>
          (_, row, i) => row.update(i, forSplit)
        case f if f.name.startsWith(VariantSchemas.infoFieldPrefix) =>
          (vc, row, i) => updateInfoField(vc, field, row, i)
        case f if f.name == VariantSchemas.genotypesFieldName =>
          val gSchema = field.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
          val gConverter = makeGenotypeConverter(gSchema)
          (vc: VariantContext, row: InternalRow, i: Int) => {
            val alleleMap = buildAlleleMap(vc)
            val output = new Array[Any](vc.getGenotypes.size())
            var j = 0
            while (j < output.length) {
              output(j) = gConverter((alleleMap, vc.getGenotype(j)))
              j += 1
            }
            row.update(i, new GenericArrayData(output))
          }
        case f =>
          logger.info(
            s"Column $f cannot be derived from VCF records. It will be null for each " +
            s"row."
          )
          (_, _, _) => ()
      }
      i += 1
    }
    new RowConverter[VariantContext](schema, fns)
  }

  private def makeGenotypeConverter(
      gSchema: StructType): RowConverter[(JMap[Allele, Int], HTSJDKGenotype)] = {
    val fns = new Array[RowConverter.Updater[(JMap[Allele, Int], HTSJDKGenotype)]](gSchema.length)
    var i = 0
    while (i < fns.length) {
      fns(i) = gSchema(i) match {
        case f if structFieldsEqualExceptNullability(f, phasedField) =>
          (el, r, i) => updateGTPhased(el._2, r, i)
        case f if structFieldsEqualExceptNullability(f, callsField) =>
          (el, r, i) => updateGTCalls(el._2, el._1, r, i)
        case f if structFieldsEqualExceptNullability(f, sampleIdField) =>
          (el, r, i) => updateSampleId(el._2, r, i)
        case f if structFieldsEqualExceptNullability(f, depthField) =>
          (el, r, i) => updateDP(el._2, r, i)
        case f if structFieldsEqualExceptNullability(f, genotypeFiltersField) =>
          (el, r, i) => updateGTFilters(el._2, r, i)
        case f if structFieldsEqualExceptNullability(f, phredLikelihoodsField) =>
          (el, r, i) => updatePL(el._2, r, i)
        case f if structFieldsEqualExceptNullability(f, alleleDepthsField) =>
          (el, r, i) => updateAD(el._2, r, i)
        case f if structFieldsEqualExceptNullability(f, conditionalQualityField) =>
          (el, r, i) => updateGQ(el._2, r, i)
        case f if structFieldsEqualExceptNullability(f, otherFieldsField) =>
          (el, r, i) => updateOtherFields(el._2, r, i)
        case f =>
          val realName = GenotypeFields.reverseAliases.getOrElse(f.name, f.name)
          (el, r, i) => updateFormatField(el._2, realName, f.dataType, r, i)
      }
      i += 1
    }
    new RowConverter[(JMap[Allele, Int], HTSJDKGenotype)](gSchema, fns)
  }

  private lazy val splitConverter = makeConverter(true)
  private lazy val nonSplitConverter = makeConverter(false)

  def convertRow(vc: VariantContext, isSplit: Boolean): InternalRow = {
    if (isSplit) {
      splitConverter(vc)
    } else {
      nonSplitConverter(vc)
    }
  }

  def convertRow(vc: VariantContext, priorRow: InternalRow, isSplit: Boolean): InternalRow = {
    if (isSplit) {
      splitConverter(vc, priorRow)
    } else {
      nonSplitConverter(vc, priorRow)
    }
  }

  private def buildAlleleMap(vc: VariantContext): JMap[Allele, Int] = {
    var alleleIdx = 0
    val alleleMap = new JHashMap[Allele, Int](vc.getAlleles.size)
    while (alleleIdx < vc.getAlleles.size()) {
      alleleMap.put(vc.getAlleles.get(alleleIdx), alleleIdx)
      alleleIdx += 1
    }
    alleleMap
  }

  private def tryWithWarning(fieldName: String, fieldType: String)(f: => Unit): Unit = {
    try {
      f
    } catch {
      case NonFatal(ex) =>
        provideWarning(
          s"Could not parse $fieldType field $fieldName. " +
          s"Exception: ${ex.getMessage}"
        )
    }
  }

  private def updateContigName(vc: VariantContext, row: InternalRow, idx: Int): Unit = {
    row.update(idx, UTF8String.fromString(vc.getContig))
  }

  private def updateStart(vc: VariantContext, row: InternalRow, idx: Int): Unit = {
    row.setLong(idx, vc.getStart.toLong - 1)
  }

  private def updateEnd(vc: VariantContext, row: InternalRow, idx: Int): Unit = {
    row.setLong(idx, vc.getEnd.toLong)
  }

  private def updateNames(vc: VariantContext, row: InternalRow, idx: Int): Unit = {
    val ids: Array[Any] = if (vc.hasID) {
      val splits = vc.getID.split(VCFConstants.ID_FIELD_SEPARATOR)
      val arr = new Array[Any](splits.length)
      var i = 0
      while (i < splits.length) {
        arr(i) = UTF8String.fromString(splits(i))
        i += 1
      }
      arr
    } else {
      Array.empty[Any]
    }
    row.update(idx, new GenericArrayData(ids))
  }

  private def updateReferenceAllele(vc: VariantContext, row: InternalRow, idx: Int): Unit = {
    row.update(idx, UTF8String.fromString(vc.getReference.getDisplayString))
  }

  private def updateAltAlleles(vc: VariantContext, row: InternalRow, idx: Int): Unit = {
    val altList = new Array[Any](vc.getAlternateAlleles.size)
    var i = 0
    while (i < altList.length) {
      altList(i) = UTF8String.fromString(vc.getAlternateAllele(i).getDisplayString)
      i += 1
    }
    row.update(idx, new GenericArrayData(altList))
  }

  private def updateQual(vc: VariantContext, row: InternalRow, idx: Int): Unit = {
    if (vc.hasLog10PError) {
      row.setDouble(idx, vc.getPhredScaledQual)
    }
  }

  private def updateFilters(vc: VariantContext, row: InternalRow, idx: Int): Unit = {
    val filters: Array[Any] = if (vc.filtersWereApplied() && vc.getFilters.isEmpty) {
      Array(UTF8String.fromString(VCFConstants.PASSES_FILTERS_v4))
    } else if (vc.filtersWereApplied()) {
      val arr = new Array[Any](vc.getFilters.size)
      var i = 0
      val it = vc.getFilters.iterator()
      while (it.hasNext) {
        arr(i) = UTF8String.fromString(it.next())
        i += 1
      }
      arr
    } else {
      Array.empty
    }
    row.update(idx, new GenericArrayData(filters))
  }

  private def updateAttributes(vc: VariantContext, row: InternalRow, idx: Int): Unit = {
    val htsjdkAttributes = vc.getAttributes
    val attKeyIterator = htsjdkAttributes.keySet().iterator
    val keys = new Array[UTF8String](htsjdkAttributes.size)
    val values = new Array[UTF8String](htsjdkAttributes.size)
    var i = 0
    while (attKeyIterator.hasNext) {
      val attKey = attKeyIterator.next()
      tryWithWarning(attKey, FieldTypes.INFO) {
        val attVal = htsjdkAttributes.get(attKey)
        val hlOpt = Option(header.getInfoHeaderLine(attKey))
        if (hlOpt.isEmpty && !infoKeysParsedWithoutHeader.contains(attKey)) {
          provideWarning(
            s"Key $attKey found in field INFO but isn't " +
            s"defined in the VCFHeader."
          )
          infoKeysParsedWithoutHeader.add(attKey)
        }
        keys(i) = UTF8String.fromString(attKey)
        val valueStr =
          VariantContextToVCFRowConverter.parseObjectAsString(obj2any(identity)(attVal))
        values(i) = UTF8String.fromString(valueStr)
      }
      i += 1
    }
    row.update(idx, new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values)))
  }

  private def updateInfoField(
      vc: VariantContext,
      field: StructField,
      row: InternalRow,
      idx: Int): Unit = {

    val realName = field.name.stripPrefix(VariantSchemas.infoFieldPrefix)
    if (!vc.hasAttribute(realName)) {
      row.setNullAt(idx)
      return
    }

    tryWithWarning(realName, FieldTypes.INFO) {
      val value: Any = field.dataType match {
        case StringType => UTF8String.fromString(vc.getAttributeAsString(realName, ""))
        case IntegerType => vc.getAttributeAsInt(realName, 0)
        case DoubleType => vc.getAttributeAsDouble(realName, 0)
        case BooleanType => true: java.lang.Boolean
        case ArrayType(StringType, _) =>
          val strings = vc.getAttributeAsStringList(realName, "")
          val arr = new Array[Any](strings.size)
          var i = 0
          while (i < strings.size) {
            arr(i) = UTF8String.fromString(strings.get(i))
            i += 1
          }
          new GenericArrayData(arr)
        case ArrayType(IntegerType, _) =>
          new GenericArrayData(vc.getAttributeAsIntList(realName, 0).asScala)
        case ArrayType(DoubleType, _) =>
          new GenericArrayData(vc.getAttributeAsDoubleList(realName, 0).asScala)
      }
      if (value != null) {
        row.update(idx, value)
      }
    }
  }

  private def updateSampleId(g: HTSJDKGenotype, row: InternalRow, offset: Int): Unit = {
    if (!writeSampleIds) {
      return
    }

    tryWithWarning("sampleId", FieldTypes.FORMAT) {
      val sampleId = UTF8String.fromString(g.getSampleName)
      row.update(offset, sampleId)
    }
  }

  private def updateGTPhased(g: HTSJDKGenotype, row: InternalRow, offset: Int): Unit = {
    tryWithWarning("GT", FieldTypes.FORMAT) {
      row.setBoolean(offset, g.isPhased)
    }
  }

  private def updateGTCalls(
      g: HTSJDKGenotype,
      alleleMap: JMap[Allele, Int],
      row: InternalRow,
      offset: Int): Unit = {
    val alleleList = g.getAlleles
    val numAlleles = alleleList.size

    tryWithWarning("GT", FieldTypes.FORMAT) {
      if (numAlleles > 0) {
        val callArray = new Array[Any](numAlleles)
        var alleleIdx = 0
        while (alleleIdx < numAlleles) {
          val allele = alleleList.get(alleleIdx)
          val call = alleleMap.getOrDefault(allele, -1)
          callArray(alleleIdx) = call
          alleleIdx += 1
        }
        row.update(offset, new GenericArrayData(callArray))
      }
    }
  }

  private def updateDP(g: HTSJDKGenotype, row: InternalRow, offset: Int): Unit = {
    tryWithWarning("DP", FieldTypes.FORMAT) {
      if (g.hasDP) {
        row.setInt(offset, g.getDP)
      }
    }
  }

  private def updateGTFilters(g: HTSJDKGenotype, row: InternalRow, idx: Int): Unit = {
    tryWithWarning("FT", FieldTypes.FORMAT) {
      if (g.isFiltered) {
        val split = g.getFilters.split(VCFConstants.FILTER_CODE_SEPARATOR)
        val arr = new Array[Any](split.length)
        var i = 0
        while (i < arr.length) {
          arr(i) = UTF8String.fromString(split(i))
          i += 1
        }
        row.update(idx, new GenericArrayData(arr))
      }
    }
  }

  private def updatePL(g: HTSJDKGenotype, row: InternalRow, idx: Int): Unit = {
    tryWithWarning("PL", FieldTypes.FORMAT) {
      if (g.hasPL) {
        row.update(idx, new GenericArrayData(g.getPL))
      }
    }
  }

  private def updateAD(g: HTSJDKGenotype, row: InternalRow, idx: Int): Unit = {
    tryWithWarning("AD", FieldTypes.FORMAT) {
      if (g.hasAD) {
        row.update(idx, new GenericArrayData(g.getAD))
      }
    }
  }

  private def updateGQ(g: HTSJDKGenotype, row: InternalRow, idx: Int): Unit = {
    tryWithWarning("GQ", FieldTypes.FORMAT) {
      if (g.hasGQ) {
        row.setInt(idx, g.getGQ)
      }
    }
  }

  private def updateFormatField(
      g: HTSJDKGenotype,
      fieldName: String,
      dataType: DataType,
      row: InternalRow,
      idx: Int): Unit = {
    val obj = g.getExtendedAttribute(fieldName)
    if (obj == null) {
      return
    }

    tryWithWarning(fieldName, FieldTypes.FORMAT) {
      val value: AnyRef = dataType match {
        case StringType => obj2any(UTF8String.fromString)(obj)
        case IntegerType => obj2any[java.lang.Integer](_.toInt)(obj)
        case DoubleType => obj2any[java.lang.Double](_.toDouble)(obj)
        case BooleanType => true: java.lang.Boolean
        case ArrayType(StringType, _) => obj2array(UTF8String.fromString)(obj)
        case ArrayType(IntegerType, _) =>
          obj2array[java.lang.Integer, Int](_.toInt)(obj, Some(Int.box))
        case ArrayType(DoubleType, _) =>
          obj2array[java.lang.Double, Double](_.toDouble)(obj, Some(Double.box))
      }

      if (value == null) {
        return
      }

      val maybeWrapped = if (dataType.isInstanceOf[ArrayType]) {
        new GenericArrayData(value)
      } else {
        value
      }

      row.update(idx, maybeWrapped)
    }
  }

  private def updateOtherFields(genotype: HTSJDKGenotype, row: InternalRow, offset: Int): Unit = {
    val excludedFields = GenotypeFields.aliases.keySet
    val extendedAttributes = genotype.getExtendedAttributes
    val keys = new Array[Any](extendedAttributes.size)
    val values = new Array[Any](extendedAttributes.size)
    val it = extendedAttributes.keySet.iterator
    var i = 0
    while (it.hasNext) {
      val key = it.next()
      if (!excludedFields.contains(key)) {
        tryWithWarning(key, FieldTypes.FORMAT) {
          val hlOpt = Option(header.getFormatHeaderLine(key))
          if (hlOpt.isEmpty && !formatKeysParsedWithoutHeader.contains(key)) {
            provideWarning(
              s"Key $key found in field FORMAT but isn't " +
              s"defined in the VCFHeader."
            )
            formatKeysParsedWithoutHeader.add(key)
          }
          val valueStr = VariantContextToVCFRowConverter.parseObjectAsString(
            obj2any(identity)(genotype.getExtendedAttribute(key))
          )
          if (valueStr.nonEmpty) {
            keys(i) = UTF8String.fromString(key)
            values(i) = UTF8String.fromString(valueStr)
            i += 1
          } else {
            provideWarning(
              s"Key $key has empty value $valueStr, but FLAG is not supported in FORMAT fields."
            )
          }
        }
      }
    }

    row.update(
      offset,
      new ArrayBasedMapData(
        new GenericArrayData(shortenArray(keys, i)),
        new GenericArrayData(shortenArray(values, i)))
    )
  }

  private def string2any[T <: AnyRef](converter: String => T)(s: String): T = s match {
    case VCFConstants.MISSING_VALUE_v4 => null.asInstanceOf[T]
    case s: String => converter(s)
  }

  private def string2list[T <: AnyRef: ClassTag](converter: String => T)(s: String): Array[Any] = {
    val split = s.split(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR_CHAR)
    var i = 0
    val out = new Array[Any](split.length)
    val it = split.iterator
    while (it.hasNext) {
      val converted = string2any(converter)(it.next)
      if (converted != null) {
        out(i) = converted
        i += 1
      }
    }
    shortenArray(out, i)
  }

  private def obj2any[T <: AnyRef: ClassTag](converter: String => T)(obj: Object): T = obj match {
    case null => null.asInstanceOf[T]
    case o: T => o
    case s: String => converter(s)
    case other: Any => converter(VariantContextToVCFRowConverter.parseObjectAsString(other))
  }

  private def obj2array[T <: AnyRef: ClassTag, R <: AnyVal](
      converter: String => T)(obj: Object, primitiveConverter: Option[R => T] = None): Array[Any] =
    obj match {
      case null => null
      case VCFConstants.MISSING_VALUE_v4 => null
      case arr: Array[T] => arr.asInstanceOf[Array[Any]]
      case arr: Array[R] if primitiveConverter.isDefined => arr.map(primitiveConverter.get)
      case l: JList[T] =>
        val arr = new Array[Any](l.size)
        var i = 0
        while (i < arr.length) {
          arr(i) = l.get(i)
          i += 1
        }
        arr
      case s: String => string2list(converter)(s)
    }

  private def shortenArray(arr: Array[Any], i: Int): Array[Any] = {
    if (i < arr.length) {
      val out = new Array[Any](i)
      Array.copy(arr, 0, out, 0, i)
      out
    } else {
      arr
    }
  }
}

object FieldTypes {
  val FORMAT: String = "FORMAT"
  val INFO: String = "INFO"
}
