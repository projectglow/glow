package org.projectglow.core.vcf

import scala.collection.JavaConverters._

import htsjdk.variant.vcf._
import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.sql.types._

import org.projectglow.core.common.{GenotypeFields, VariantSchemas}

/**
 * Infers the schema of a VCF file from its headers.
 */
object VCFSchemaInferrer {

  def getInfoFieldStruct(headerLine: VCFInfoHeaderLine): StructField = {
    StructField(
      VariantSchemas.infoFieldPrefix + headerLine.getID,
      typesForHeader(headerLine).head,
      metadata = metadataForLine(headerLine))
  }

  /**
   * @param includeSampleIds If true, a sampleId column will be added to the genotype fields
   * @param flattenInfoFields If true, each INFO field will be promoted to a column. If false,
   *                          they will instead be stored in a string -> string map
   * @return A StructType describing the schema
   */
  def inferSchema(
      includeSampleIds: Boolean,
      flattenInfoFields: Boolean,
      infoHeaders: Seq[VCFInfoHeaderLine],
      formatHeaders: Seq[VCFFormatHeaderLine]): StructType = {
    val validatedInfoHeaders = validateHeaders(infoHeaders)
    val validatedFormatHeaders = validateHeaders(formatHeaders)
    val withInfoFields = if (flattenInfoFields) {
      validatedInfoHeaders.foldLeft(VariantSchemas.vcfBaseSchema) {
        case (schema, line) =>
          val field = getInfoFieldStruct(line)
          schema.add(field)
      }
    } else {
      VariantSchemas.vcfBaseSchema.add(StructField("attributes", MapType(StringType, StringType)))
    }

    var genotypeStruct = StructType(Seq.empty)
    if (includeSampleIds) {
      genotypeStruct = genotypeStruct.add(VariantSchemas.sampleIdField)
    }

    validatedFormatHeaders.foreach { line =>
      val names = GenotypeFields.aliases.getOrElse(line.getID, Seq(line.getID))
      val types = typesForHeader(line)
      require(
        names.size == types.size,
        "Must have same number of header line struct names and types"
      )
      val metadata = metadataForLine(line)
      names.zip(types).foreach {
        case (n, t) =>
          genotypeStruct = genotypeStruct.add(StructField(n, t, metadata = metadata))
      }
    }

    withInfoFields.add(StructField("genotypes", ArrayType(genotypeStruct)))
  }

  def inferSchema(
      includeSampleIds: Boolean,
      flattenInfoFields: Boolean,
      header: VCFHeader): StructType = {
    inferSchema(
      includeSampleIds,
      flattenInfoFields,
      header.getInfoHeaderLines.asScala.toSeq,
      header.getFormatHeaderLines.asScala.toSeq)
  }

  /**
   * Returns the VCF header lines that correspond to a variant schema. Each flattened info field
   * (those fields whose names start with "INFO_") will be converted to an info header line, and
   * fields from the "genotype" struct will be converted to format header lines.
   *
   * If the count type is available in the schema metadata (which is always the case if the original
   * schema was generated by `inferSchema`), that will be the returned count type. If not, we
   * provide a best guess count type according to the following schema possibilities:
   * - If it's a boolean field, return count = 0, as is the convention for flags
   * - If it's a non-array field, return count = 1
   * - If it's an array field, return count = UNBOUNDED
   * @param schema The schema of the variant DataFrame
   * @return VCF header lines that can be inferred from the input schema
   */
  def headerLinesFromSchema(schema: StructType): Seq[VCFHeaderLine] = {
    val infoLines = schema.filter(_.name.startsWith(VariantSchemas.infoFieldPrefix)).map { field =>
      val name = field.name.stripPrefix(VariantSchemas.infoFieldPrefix)
      val vcfType = vcfDataType(field.dataType)
      val (countType, count) = vcfHeaderLineCount(field)
      val description = vcfHeaderLineDescription(field)
      makeHeaderLine(name, vcfType, countType, count, description, isFormat = false)
    }

    val formatLines = if (schema.exists(_.name == VariantSchemas.genotypesFieldName)) {
      val gSchema = schema
        .find(_.name == VariantSchemas.genotypesFieldName)
        .get
        .dataType
        .asInstanceOf[ArrayType]
        .elementType
        .asInstanceOf[StructType]
      gSchema.flatMap {
        case field
            if Set(VariantSchemas.otherFieldsField.name, VariantSchemas.sampleIdField.name)
              .contains(field.name) =>
          None
        case field if getSpecialHeaderLine(field.name).isDefined =>
          getSpecialHeaderLine(field.name)
        case field =>
          val name = GenotypeFields.reverseAliases.getOrElse(field.name, field.name)
          val vcfType = vcfDataType(field.dataType)
          val (countType, count) = vcfHeaderLineCount(field)
          val description = vcfHeaderLineDescription(field)
          Some(makeHeaderLine(name, vcfType, countType, count, description, isFormat = true))
      }.distinct
    } else {
      Seq.empty
    }

    infoLines ++ formatLines
  }

  def typesForHeader(line: VCFCompoundHeaderLine): Seq[DataType] = {
    if (particularSchemas.contains(line.getID)) {
      return particularSchemas(line.getID)
    }

    val primitiveType = line.getType match {
      case VCFHeaderLineType.Character => StringType
      case VCFHeaderLineType.String => StringType
      case VCFHeaderLineType.Float => DoubleType
      case VCFHeaderLineType.Integer => IntegerType
      case VCFHeaderLineType.Flag => BooleanType
    }

    if (line.isFixedCount && line.getCount <= 1) {
      Seq(primitiveType)
    } else {
      Seq(ArrayType(primitiveType))
    }
  }

  private def getSpecialHeaderLine(fieldName: String): Option[VCFHeaderLine] = {
    if (Set(VariantSchemas.callsField.name, VariantSchemas.phasedField.name).contains(fieldName)) {
      Some(VCFRowHeaderLines.genotype)
    } else {
      None
    }
  }

  private def vcfDataType(dt: DataType): VCFHeaderLineType = dt match {
    case StringType => VCFHeaderLineType.String
    case DoubleType => VCFHeaderLineType.Float
    case IntegerType => VCFHeaderLineType.Integer
    case BooleanType => VCFHeaderLineType.Flag
    case ArrayType(innerType, _) => vcfDataType(innerType)
  }

  /**
   * Infer the VCF count type for a struct field.
   * @return If the count type is `INTEGER`, then the second parameter will be the actual count. If
   *         not, it will be None.
   */
  private def vcfHeaderLineCount(field: StructField): (VCFHeaderLineCount, Option[Int]) = {
    if (field.metadata.contains(VCF_HEADER_COUNT_KEY)) {
      val countStr = field.metadata.getString(VCF_HEADER_COUNT_KEY)
      if (NumberUtils.isDigits(countStr)) {
        (VCFHeaderLineCount.INTEGER, Some(countStr.toInt))
      } else {
        (VCFHeaderLineCount.valueOf(field.metadata.getString(VCF_HEADER_COUNT_KEY)), None)
      }
    } else if (field.dataType.isInstanceOf[ArrayType]) {
      (VCFHeaderLineCount.UNBOUNDED, None)
    } else if (field.dataType == BooleanType) {
      (VCFHeaderLineCount.INTEGER, Some(0))
    } else {
      (VCFHeaderLineCount.INTEGER, Some(1))
    }
  }

  private def vcfHeaderLineDescription(field: StructField): String = {
    if (field.metadata.contains(VCF_HEADER_DESCRIPTION_KEY)) {
      field.metadata.getString(VCF_HEADER_DESCRIPTION_KEY)
    } else {
      ""
    }
  }

  private def metadataForLine(line: VCFCompoundHeaderLine): Metadata = {
    val numberStr: String = if (line.getCountType == VCFHeaderLineCount.INTEGER) {
      line.getCount.toString
    } else {
      line.getCountType.name()
    }
    new MetadataBuilder()
      .putString(VCF_HEADER_COUNT_KEY, numberStr)
      .putString(VCF_HEADER_DESCRIPTION_KEY, line.getDescription)
      .build()
  }

  private def makeHeaderLine(
      name: String,
      vcfType: VCFHeaderLineType,
      countType: VCFHeaderLineCount,
      count: Option[Int],
      description: String,
      isFormat: Boolean): VCFCompoundHeaderLine = (countType, isFormat) match {
    case (VCFHeaderLineCount.INTEGER, true) =>
      new VCFFormatHeaderLine(name, count.get, vcfType, description)
    case (VCFHeaderLineCount.INTEGER, false) =>
      new VCFInfoHeaderLine(name, count.get, vcfType, description)
    case (cntType, true) =>
      new VCFFormatHeaderLine(name, cntType, vcfType, description)
    case (cntType, false) =>
      new VCFInfoHeaderLine(name, cntType, vcfType, description)
  }

  /**
   * Given a group of headers, ensures that there are no incompatible duplicates (e.g., same name
   * but different type or count).
   * @return A seq of unique headers
   */
  private def validateHeaders[A <: VCFCompoundHeaderLine](headers: Seq[A]): Seq[A] = {
    headers
      .groupBy(line => line.getID)
      .map {
        case (id, lines) =>
          if (!lines.tail.forall(_.equalsExcludingDescription(lines.head))) {
            // Some headers with same key but different types
            throw new IllegalArgumentException(s"VCF headers with id $id have incompatible schemas")
          }
          lines.head
      }
      .toSeq
  }

  // Fields for which the schema cannot be inferred from the VCF header
  private val particularSchemas: Map[String, Seq[DataType]] = Map(
    "GT" -> Seq(VariantSchemas.phasedField.dataType, VariantSchemas.callsField.dataType)
  )

  // Public constants
  val VCF_HEADER_COUNT_KEY = "vcf_header_count"
  val VCF_HEADER_DESCRIPTION_KEY = "vcf_header_description"
}
