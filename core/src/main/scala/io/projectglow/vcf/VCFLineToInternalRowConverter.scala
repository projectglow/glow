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

import java.util
import java.util.Collections

import com.google.common.base.Splitter
import htsjdk.samtools.ValidationStringency
import htsjdk.samtools.util.OverlapDetector
import htsjdk.variant.vcf.VCFHeader
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.unsafe.types.UTF8String.{IntWrapper, LongWrapper}
import io.projectglow.common.{GenotypeFields, HasStringency, SimpleInterval, VariantSchemas}

import scala.util.control.NonFatal

/**
 * Converts the raw bytes in a VCF line into an [[InternalRow]]
 *
 * @param header The VCF header object this is currently only used to extract sample ids
 * @param schema The schema of the converted rows
 * @param stringency How to handle errors
 * @param overlapDetectorOpt If provided, the converter will check to see if a variant passes this detector before
 *                           parsing genotypes
 */
class VCFLineToInternalRowConverter(
    header: VCFHeader,
    schema: StructType,
    val stringency: ValidationStringency,
    overlapDetectorOpt: Option[OverlapDetector[SimpleInterval]])
    extends HasStringency {

  private val genotypeHolder = new Array[Any](header.getNGenotypeSamples)

  private def findFieldIdx(field: StructField): Int = {
    schema.indexWhere(SQLUtils.structFieldsEqualExceptNullability(_, field))
  }
  private val contigIdx = findFieldIdx(VariantSchemas.contigNameField)
  private val startIdx = findFieldIdx(VariantSchemas.startField)
  private val namesIdx = findFieldIdx(VariantSchemas.namesField)
  private val refAlleleIdx = findFieldIdx(VariantSchemas.refAlleleField)
  private val altAllelesIdx = findFieldIdx(VariantSchemas.alternateAllelesField)
  private val qualIdx = findFieldIdx(VariantSchemas.qualField)
  private val filtersIdx = findFieldIdx(VariantSchemas.filtersField)
  private val endIdx = findFieldIdx(VariantSchemas.endField)
  private val genotypesIdx = schema.indexWhere(_.name == VariantSchemas.genotypesFieldName)
  private val splitFromMultiIdx = findFieldIdx(VariantSchemas.splitFromMultiAllelicField)
  private lazy val sampleIds = header
    .getGenotypeSamples
    .toArray()
    .map(el => UTF8String.fromString(el.asInstanceOf[String]))

  private val infoFields = schema
    .zipWithIndex
    .collect {
      case (f, idx) if f.name.startsWith("INFO_") =>
        val name = f.name.stripPrefix("INFO_")
        val typ = f.dataType
        (UTF8String.fromString(name), (typ, idx))
    }
    .toMap

  private val flagFields = infoFields.filter { f =>
    f._2._1 == BooleanType
  }

  private val gSchemaOpt = schema
    .find(_.name == VariantSchemas.genotypesFieldName)
    .map(_.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])

  private var callsIdx = -1
  private var phasedIdx = -1
  private var sampleIdIdx = -1
  private val genotypeFieldsOpt: Option[Map[UTF8String, (DataType, Int)]] = gSchemaOpt.map {
    schema =>
      schema
        .zipWithIndex
        .flatMap {
          case (gf, idx) =>
            if (SQLUtils.structFieldsEqualExceptNullability(gf, VariantSchemas.sampleIdField)) {
              sampleIdIdx = idx
              None
            } else if (SQLUtils.structFieldsEqualExceptNullability(gf, VariantSchemas.callsField)) {
              callsIdx = idx
              None
            } else if (SQLUtils.structFieldsEqualExceptNullability(gf, VariantSchemas.phasedField)) {
              phasedIdx = idx
              None
            } else {
              val vcfName = GenotypeFields.reverseAliases.getOrElse(gf.name, gf.name)
              val typ = gf.dataType
              Some((UTF8String.fromString(vcfName), (typ, idx)))
            }
        }
        .toMap
  }

  private def set(row: InternalRow, idx: Int, value: Any): Unit = {
    if (idx == -1) {
      return
    }
    row.update(idx, value)
  }

  /**
   * Converts a VCF line into an [[InternalRow]]
   * @param line A text object containing the VCF line
   * @return The converted row or null to represent a parsing failure or filtered row
   */
  def convert(line: Text): InternalRow = {
    var contigName: UTF8String = null
    var start: Long = -1
    var end: Long = -1
    val row = new GenericInternalRow(schema.size)

    set(row, splitFromMultiIdx, false)
    // By default, FLAG fields should be false
    flagFields.foreach {
      case (key, (typ, idx)) =>
        set(row, idx, false)
    }

    val ctx = new LineCtx(line)
    if (ctx.isHeader) {
      return null
    }

    contigName = ctx.parseString()
    set(row, contigIdx, contigName)
    ctx.expectTab()

    start = ctx.parseLong() - 1
    set(row, startIdx, start)
    ctx.expectTab()

    val names = ctx.parseStringArray()
    set(row, namesIdx, ctx.toGenericArrayData(names))
    ctx.expectTab()

    val refAllele = ctx.parseString()
    set(row, refAlleleIdx, refAllele)
    end = start + refAllele.numChars()
    set(row, endIdx, end)
    ctx.expectTab()

    val altAlleles = ctx.parseStringArray()
    set(row, altAllelesIdx, ctx.toGenericArrayData(altAlleles))
    ctx.expectTab()

    val qual = ctx.parseDouble()
    set(row, qualIdx, qual)
    ctx.expectTab()

    val filters = ctx.parseStringArray()
    set(row, filtersIdx, ctx.toGenericArrayData(filters))
    ctx.expectTab()

    while (!ctx.isTab) {
      val key = ctx.parseString('=', ';')
      tryWithWarning(key, FieldTypes.INFO) {
        ctx.eat('=')
        if (key == UTF8String.fromString("END")) {
          end = ctx.parseInfoVal(LongType).asInstanceOf[Long]
          set(row, endIdx, end)
        } else if (!infoFields.contains(key)) {
          ctx.parseString(';')
        } else {
          val (typ, idx) = infoFields(key)
          val value = ctx.parseInfoVal(typ)
          set(row, idx, value)
        }
      }
      ctx.eat(';')
    }

    if (overlapDetectorOpt.isDefined) {
      val contigStr = if (contigName == null) null else contigName.toString
      val interval = SimpleInterval(contigStr, start.toInt + 1, end.toInt)
      if (!overlapDetectorOpt.get.overlapsAny(interval)) {
        return null
      }
    }

    if (genotypeFieldsOpt.isEmpty) {
      return row
    }

    if (ctx.isLineEnd) {
      return row
    }

    ctx.expectTab()
    row.update(genotypesIdx, parseGenotypes(ctx))

    row
  }

  /**
   * Parses the genotypes from the format section of a VCF line.
   *
   * Basic flow:
   * - Look at the format description block and map each field to an index in the genotype SQL schema and a data type
   * - Iterate through the fields for each sample and update the genotype struct based on the stored indices and types
   * @param ctx
   * @return
   */
  private def parseGenotypes(ctx: LineCtx): GenericArrayData = {
    val gSchema = gSchemaOpt.get
    val genotypeFields = genotypeFieldsOpt.get
    val fieldNames = ctx.parseStringArray(':')
    var gtIdx = -1
    val typeAndIdx: Array[(DataType, Int)] = new Array[(DataType, Int)](fieldNames.length)

    var i = 0
    while (i < typeAndIdx.length) {
      val name = fieldNames(i).asInstanceOf[UTF8String]
      // GT maps to two fields, so special case it
      if (name.toString == "GT") {
        gtIdx = i
      }
      if (genotypeFields.contains(name)) {
        typeAndIdx(i) = genotypeFields(name)
      }
      i += 1
    }
    ctx.expectTab()
    var sampleIdx = 0
    while (!ctx.isLineEnd && sampleIdx < genotypeHolder.length) {
      val gRow = new GenericInternalRow(gSchema.size)
      if (sampleIdIdx != -1) {
        gRow.update(sampleIdIdx, sampleIds(sampleIdx))
      }
      var i = 0
      while (!ctx.isTab && i < typeAndIdx.length) {
        tryWithWarning(fieldNames(i).asInstanceOf[UTF8String], FieldTypes.FORMAT) {
          if (i == gtIdx) {
            ctx.parseCallsAndPhasing(gRow, phasedIdx, callsIdx)
          } else if (typeAndIdx(i) == null) {
            // Eat this value as a string since we don't need the parsed value
            ctx.parseString(':')
          } else {
            val (typ, idx) = typeAndIdx(i)
            val value = ctx.parseFormatVal(typ)
            gRow.update(idx, value)
          }
          ctx.eat(':')
          i += 1
        }
      }
      ctx.eat('\t')
      genotypeHolder(sampleIdx) = gRow
      sampleIdx += 1
    }
    new GenericArrayData(genotypeHolder)
  }

  private def tryWithWarning(fieldName: UTF8String, fieldType: String)(f: => Unit): Unit = {
    try {
      f
    } catch {
      case NonFatal(ex) =>
        raiseValidationError(
          s"Could not parse $fieldType field ${fieldName.toString}. " +
          s"Exception: ${ex.getMessage}",
          ex
        )
    }
  }
}

class LineCtx(text: Text) {
  val line = text.getBytes
  var pos = 0
  var delimiter = '\0' // unset

  val longWrapper = new LongWrapper()
  val intWrapper = new IntWrapper()

  val stringList = new util.ArrayList[UTF8String]()
  val intList = new util.ArrayList[java.lang.Integer]()
  val longList = new util.ArrayList[java.lang.Long]()
  val doubleList = new util.ArrayList[java.lang.Double]()

  def setInInfoVal(): Unit = {
    delimiter = ';'
  }

  def setInFormatVal(): Unit = {
    delimiter = ':'
  }

  def printRemaining(): Unit = {
    println(new String(line.slice(pos, text.getLength), "UTF-8")) // scalastyle:ignore
  }

  def isHeader: Boolean = {
    line.isEmpty || line(0) == '#'
  }

  def parseString(extraStopChar1: Byte = '\0', extraStopChar2: Byte = '\0'): UTF8String = {
    var stop = pos
    while (stop < text.getLength && line(stop) != delimiter && line(stop) != '\t' && line(stop) != extraStopChar1 && line(
        stop) != extraStopChar2) {
      stop += 1
    }

    if (stop - pos == 0) {
      return null
    }

    val out = UTF8String.fromBytes(line, pos, stop - pos)
    pos = stop
    if (out == LineCtx.MISSING) {
      null
    } else {
      out
    }
  }

  def isTab: Boolean = {
    pos >= text.getLength || line(pos) == '\t'
  }

  def isDelimiter: Boolean = {
    pos >= text.getLength || line(pos) == delimiter
  }

  def isLineEnd: Boolean = {
    pos >= text.getLength || line(pos) == '\n' || line(pos) == '\r'
  }

  def expectTab(): Unit = {
    if (line(pos) != '\t') {
      throw new IllegalStateException(s"Expected a tab at position $pos")
    }
    pos += 1
  }

  def eat(char: Byte): Unit = {
    if (pos < text.getLength && line(pos) == char) {
      pos += 1
    }
  }

  def parseLong(): java.lang.Long = {
    val s = parseString()
    if (s == null) {
      return null
    }
    require(s.toLong(longWrapper), s"Could not parse field as long")
    longWrapper.value
  }

  def parseInt(
      stopChar1: Byte = '\0',
      stopChar2: Byte = '\0',
      nullValue: java.lang.Integer = null): java.lang.Integer = {
    val s = parseString(stopChar1, stopChar2)
    if (s == null) {
      return nullValue
    }
    require(s.toInt(intWrapper), s"Could not parse field as int")
    intWrapper.value
  }

  def parseDouble(stopChar: Byte = '\0'): java.lang.Double = {
    val utfStr = parseString(stopChar)
    if (utfStr == null) {
      return null
    }
    val s = utfStr.toLowerCase

    if (s == LineCtx.NAN) {
      Double.NaN
    } else if (s == LineCtx.NEG_NAN) {
      Double.NaN
    } else if (s == LineCtx.POS_NAN) {
      Double.NaN
    } else if (s == LineCtx.INF || s == LineCtx.INFINITY) {
      Double.PositiveInfinity
    } else if (s == LineCtx.POS_INF || s == LineCtx.POS_INFINITY) {
      Double.PositiveInfinity
    } else if (s == LineCtx.NEG_INF || s == LineCtx.NEG_INFINITY) {
      Double.NegativeInfinity
    } else {
      s.toString.toDouble
    }
  }

  def parseStringArray(sep: Byte = ','): Array[AnyRef] = {
    stringList.clear()
    while (!isLineEnd && !isTab && !isDelimiter) {
      stringList.add(parseString(sep))
      eat(sep)
    }
    stringList.toArray()
  }

  def parseIntArray(): Array[AnyRef] = {
    intList.clear()
    while (!isLineEnd && !isTab && !isDelimiter) {
      intList.add(parseInt(','))
      eat(',')
    }
    intList.toArray()
  }

  def parseDoubleArray(): Array[AnyRef] = {
    doubleList.clear()
    while (!isLineEnd && !isTab && !isDelimiter) {
      doubleList.add(parseDouble(','))
      eat(',')
    }
    doubleList.toArray()
  }

  def toGenericArrayData(arr: Array[_]): GenericArrayData = {
    if (arr.length == 0) {
      null
    } else if (allNull(arr)) {
      if (arr.length == 1) {
        null
      } else {
        // This doesn't really make sense... We only represent a multiple element array with all nulls as an empty list
        // for consistency with probabilities read by the BGEN reader.
        // TODO(hhd): Fix the BGEN reader to properly insert nulls
        new GenericArrayData(Array.empty[Any])
      }
    } else {
      new GenericArrayData(arr.asInstanceOf[Array[Any]])
    }
  }

  private def allNull(arr: Array[_]): Boolean = {
    var i = 0
    while (i < arr.length) {
      if (arr(i) != null) {
        return false
      }
      i += 1
    }
    true
  }

  def parseByType(typ: DataType): Any = {
    if (typ == IntegerType) {
      parseInt()
    } else if (typ == LongType) {
      parseLong()
    } else if (typ == BooleanType) {
      true
    } else if (typ == StringType) {
      parseString()
    } else if (typ == DoubleType) {
      parseDouble()
    } else if (SQLUtils.dataTypesEqualExceptNullability(typ, ArrayType(StringType))) {
      toGenericArrayData(parseStringArray())
    } else if (SQLUtils.dataTypesEqualExceptNullability(typ, ArrayType(IntegerType))) {
      toGenericArrayData(parseIntArray())
    } else if (SQLUtils.dataTypesEqualExceptNullability(typ, ArrayType(DoubleType))) {
      toGenericArrayData(parseDoubleArray())
    } else if (typ.isInstanceOf[ArrayType] && typ
        .asInstanceOf[ArrayType]
        .elementType
        .isInstanceOf[StructType]) {
      val strings = parseStringArray()
      val list = new util.ArrayList[String](strings.length)
      var i = 0
      while (i < strings.length) {
        list.add(strings(i).toString)
        i += 1
      }
      new GenericArrayData(
        VariantContextToInternalRowConverter.getAnnotationArray(
          list,
          typ.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]))
    } else {
      null
    }
  }

  def parseInfoVal(typ: DataType): Any = {
    setInInfoVal()
    parseByType(typ)
  }

  def parseFormatVal(typ: DataType): Any = {
    setInFormatVal()
    parseByType(typ)
  }

  def parseCallsAndPhasing(row: GenericInternalRow, phasedIdx: Int, callsIdx: Int): Unit = {
    setInFormatVal()
    intList.clear()
    val first = parseInt('|', '/', -1)
    intList.add(first)
    var phased = false
    if (line(pos) == '|') {
      phased = true
    }
    eat('/')
    eat('|')
    while (!isTab && !isDelimiter) {
      intList.add(parseInt('|', '/', -1))
      eat('|')
      eat('/')
    }

    if (phasedIdx != -1) {
      row.update(phasedIdx, phased)
    }

    if (callsIdx != -1) {
      row.update(callsIdx, new GenericArrayData(intList.toArray().asInstanceOf[Array[Any]]))
    }
  }
}

/**
 * https://samtools.github.io/hts-specs/VCFv4.3.pdf
 * Infinity/NAN values should match the following regex:
 * ^[-+]?(INF|INFINITY|NAN)$ case insensitively
 */
object LineCtx {
  val INF = UTF8String.fromString("inf")
  val POS_INF = UTF8String.fromString("+inf")
  val NEG_INF = UTF8String.fromString("-inf")
  val INFINITY = UTF8String.fromString("infinity")
  val POS_INFINITY = UTF8String.fromString("+infinity")
  val NEG_INFINITY = UTF8String.fromString("-infinity")
  val NAN = UTF8String.fromString("nan")
  val POS_NAN = UTF8String.fromString("+nan")
  val NEG_NAN = UTF8String.fromString("-nan")
  val END = UTF8String.fromString("END")
  val MISSING = UTF8String.fromString(".")
}
