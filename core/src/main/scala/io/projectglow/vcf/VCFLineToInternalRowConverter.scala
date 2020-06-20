package io.projectglow.vcf

import java.util

import com.google.common.base.Splitter
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFHeader
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SQLUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.unsafe.types.UTF8String.{IntWrapper, LongWrapper}

import io.projectglow.common.{GenotypeFields, VariantSchemas}

class VCFLineToInternalRowConverter(
  header: VCFHeader,
  schema: StructType,
  val stringency: ValidationStringency,
  writeSampleIds: Boolean = true) {

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
  private lazy val sampleIds = header.getSampleNamesInOrder.toArray()
    .map(el => UTF8String.fromString(el.asInstanceOf[String]))

  private val infoFields = schema
    .zipWithIndex
    .collect { case (f, idx) if f.name.startsWith("INFO_") =>
      val name = f.name.stripPrefix("INFO_")
      val typ = f.dataType
      (UTF8String.fromString(name), (typ, idx))
    }.toMap

  private val gSchemaOpt = schema.find(_.name == VariantSchemas.genotypesFieldName)
    .map(_.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])


  private var callsIdx = -1
  private var phasedIdx = -1
  private var sampleIdIdx = -1
  private val genotypeFieldsOpt: Option[Map[UTF8String, (DataType, Int)]] = gSchemaOpt.map { schema =>
    schema.zipWithIndex.flatMap { case (gf, idx) =>
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
    }.toMap
  }

  private def set(row: InternalRow, idx: Int, value: Any): Unit = {
    if (idx == -1) {
      return
    }
    row.update(idx, value)
  }

  def convert(line: Text): InternalRow = {
    val row = new GenericInternalRow(schema.size)
    set(row, splitFromMultiIdx, false)
    val ctx = new LineCtx(line)
    if (ctx.isHeader) {
      return null
    }

    val contig = ctx.parseString()
    set(row, contigIdx, contig)
    ctx.expectTab()

    val start = ctx.parseLong() - 1
    set(row, startIdx, start)
    ctx.expectTab()

    val names = ctx.parseStringArray()
    set(row, namesIdx, names)
    ctx.expectTab()

    val refAllele = ctx.parseString()
    set(row, refAlleleIdx, refAllele)
    set(row, endIdx, start + refAllele.numChars())
    ctx.expectTab()

    val altAlleles = ctx.parseStringArray()
    set(row, altAllelesIdx, altAlleles)
    ctx.expectTab()

    val qual = ctx.parseDouble()
    set(row, qualIdx, qual)
    ctx.expectTab()

    val filters = ctx.parseStringArray()
    set(row, filtersIdx, filters)
    ctx.expectTab()

    while (!ctx.isTab) {
      val key = ctx.parseString('=')
      ctx.eat('=')
      if (!infoFields.contains(key)) {
        ctx.parseString(';')
      } else {
        val (typ, idx) = infoFields(key)
        val value = ctx.parseInfoVal(typ)
        row.update(idx, value)
      }
      ctx.eat(';')
    }

    if (genotypeFieldsOpt.isEmpty) {
      return row
    }

    if (ctx.isLineEnd) {
      row.update(genotypesIdx, new GenericArrayData(Array.empty[Any]))
      return row
    }

    ctx.expectTab()
    row.update(genotypesIdx, parseGenotypes(ctx))

    row
  }

  private def parseGenotypes(ctx: LineCtx): GenericArrayData = {
    val gSchema = gSchemaOpt.get
    val genotypeFields = genotypeFieldsOpt.get
    val fieldNames = ctx.parseStringArray(':').toArray(StringType)
    var gtIdx = -1
    val typeAndIdx: Array[(DataType, Int)] = new Array[(DataType, Int)](fieldNames.length)

    var i = 0
    while (i < typeAndIdx.length) {
      val name = fieldNames(i).asInstanceOf[UTF8String]
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
        if (i == gtIdx) {
          ctx.parseCallsAndPhasing(gRow, phasedIdx, callsIdx)
        } else if (typeAndIdx(i) == null) {
          // Eat this value as a string
          ctx.parseString(':')
        } else {
          val (typ, idx) = typeAndIdx(i)
          val value = ctx.parseFormatVal(typ)
          gRow.update(idx, value)
        }
        ctx.eat(':')
        i += 1
      }
      ctx.eat('\t')
      genotypeHolder(sampleIdx) = gRow
      sampleIdx += 1
    }
    new GenericArrayData(genotypeHolder)
  }
}

class LineCtx(text: Text) {
  val line = text.getBytes
  var pos = 0
  var delimiter = '\0' // unset

  val longWrapper = new LongWrapper()
  val intWrapper = new IntWrapper()

  val stringList = new util.ArrayList[UTF8String]()
  val intList = new util.ArrayList[Int]()
  val longList = new util.ArrayList[Long]()
  val doubleList = new util.ArrayList[Double]()

  def setInInfoVal(): Unit = {
    delimiter = ';'
  }

  def setInFormatVal(): Unit = {
    delimiter = ':'
  }

  def printRemaining(): Unit = {
    println(new String(line.slice(pos, text.getLength), "UTF-8"))
  }

  def isHeader: Boolean = {
    line.isEmpty || line(0) == '#'
  }

  def parseString(
    extraStopChar1: Byte = '\0',
    extraStopChar2: Byte = '\0'): UTF8String = {
    if (line(pos) == '.') {
      pos += 1
      null
    } else {
      var stop = pos
      while (stop < text.getLength && line(stop) != delimiter && line(stop) != '\t' && line(stop) != extraStopChar1 && line(stop) != extraStopChar2) {
        stop += 1
      }
      val out = UTF8String.fromBytes(line, pos, stop - pos)
      pos = stop
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
    pos >= text.getLength || line(pos) == '\n'
  }

  def expectTab(): Unit = {
    if (line(pos) != '\t') {
      throw new RuntimeException()
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
    s.toLong(longWrapper)
    longWrapper.value
  }

  def parseInt(stopChar1: Byte = '\0', stopChar2: Byte = '\0', nullValue: java.lang.Integer = null): java.lang.Integer = {
    val s = parseString(stopChar1, stopChar2)
    if (s == null) {
      return nullValue
    }
    s.toInt(intWrapper)
    intWrapper.value
  }

  def parseDouble(stopChar: Byte = '\0'): java.lang.Double = {
    val utfStr = parseString(stopChar)
    if (utfStr == null) {
      return null
    }
    val s = utfStr.toLowerCase.toString

    if (s == "nan") {
      Double.NaN
    } else if (s == "-nan") {
      Double.NaN
    } else if (s == "inf") {
      Double.PositiveInfinity
    } else if (s == "-inf") {
      Double.NegativeInfinity
    } else {
      s.toDouble
    }
  }

  def parseStringArray(sep: Byte = ','): GenericArrayData = {
    stringList.clear()
    while (!isTab) {
      stringList.add(parseString(sep))
      eat(sep)
    }
    toGenericArrayData(stringList.toArray())
  }

  def parseIntArray(): GenericArrayData = {
    intList.clear()
    while (!isTab && !isDelimiter) {
      intList.add(parseInt(','))
      eat(',')
    }
    toGenericArrayData(intList.toArray())
  }

  def parseDoubleArray(): GenericArrayData = {
    doubleList.clear()
    while (!isTab && !isDelimiter) {
      doubleList.add(parseDouble(','))
      eat(',')
    }
    toGenericArrayData(doubleList.toArray())
  }

  private def toGenericArrayData(arr: Array[_]): GenericArrayData = {
    if (arr.length == 1 && arr(0) == null) {
      new GenericArrayData(Array.empty[Any])
    } else {
      new GenericArrayData(arr.asInstanceOf[Array[Any]])
    }
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
      parseStringArray()
    } else if (SQLUtils.dataTypesEqualExceptNullability(typ, ArrayType(IntegerType))) {
      parseIntArray()
    } else if (SQLUtils.dataTypesEqualExceptNullability(typ, ArrayType(DoubleType))) {
      parseDoubleArray()
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

  def parseCallsAndPhasing(
    row: GenericInternalRow,
    phasedIdx: Int,
    callsIdx: Int): Unit = {
    setInFormatVal()
    intList.clear()
    val first = parseInt('|', '/', -1)
    intList.add(first)
    if (phasedIdx != -1) {
      if (line(pos) == '|') {
        row.update(phasedIdx, true)
      } else if (line(pos) == '/') {
        row.update(phasedIdx, false)
      }
    }
    eat('/')
    eat('|')
    while (!isTab && !isDelimiter) {
      intList.add(parseInt('|', '/', -1))
      eat('|')
      eat('/')
    }
    if (callsIdx != -1) {
      row.update(callsIdx, new GenericArrayData(intList.toArray().asInstanceOf[Array[Any]]))
    }
  }
}
