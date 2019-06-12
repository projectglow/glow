package com.databricks.vcf

import htsjdk.variant.vcf._
import org.apache.spark.sql.types._

import com.databricks.hls.common.HLSLogging

/**
 * Infers the schema of a VCF file from its headers.
 */
object VCFSchemaInferer extends HLSLogging {

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
          val field = StructField("INFO_" + line.getID, typesForHeader(line).head)
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
      names.zip(types).foreach {
        case (n, t) =>
          genotypeStruct = genotypeStruct.add(StructField(n, t))
      }
    }

    withInfoFields.add(StructField("genotypes", ArrayType(genotypeStruct)))
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

  /**
   * Given a group of headers, ensures that there are no incompatible duplicates (e.g., same name
   * but different type or count).
   * @return A seq of unique headers
   */
  private def validateHeaders(headers: Seq[VCFCompoundHeaderLine]): Seq[VCFCompoundHeaderLine] = {
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
}
