package com.databricks.vcf

import org.apache.spark.sql.{Encoders, SQLUtils}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._

object VariantSchemas {
  // Default fields common to VCF and BGEN
  val contigNameField = StructField("contigName", StringType)
  val startField = StructField("start", LongType)
  val endField = StructField("end", LongType)
  val refAlleleField = StructField("referenceAllele", StringType)
  val alternateAllelesField = StructField("alternateAlleles", ArrayType(StringType))
  val namesField = StructField("names", ArrayType(StringType))
  val genotypesFieldName = "genotypes"

  // Fields that are always present in VCF records
  val qualField = StructField("qual", DoubleType)
  val filtersField = StructField("filters", ArrayType(StringType))
  val splitFromMultiAllelicField = StructField("splitFromMultiAllelic", BooleanType)
  val attributesField = StructField("attributes", MapType(StringType, StringType))
  val infoFieldPrefix = "INFO_"

  val vcfBaseSchema: StructType = StructType(
    Seq(
      contigNameField,
      startField,
      endField,
      namesField,
      refAlleleField,
      alternateAllelesField,
      qualField,
      filtersField,
      splitFromMultiAllelicField
    )
  )

  // Possible genotype fields common to VCF and BGEN
  val sampleIdField = StructField("sampleId", StringType)
  val phasedField = StructField("phased", BooleanType)
  val posteriorProbabilitiesField = StructField("posteriorProbabilities", ArrayType(DoubleType))

  // Possible genotype fields for VCF
  val callsField = StructField("calls", ArrayType(IntegerType))
  val depthField = StructField("depth", IntegerType)
  val genotypeFiltersField = StructField("filters", ArrayType(StringType))
  val phredLikelihoodsField = StructField("phredLikelihoods", ArrayType(IntegerType))
  val genotypeLikelihoodsField = StructField("genotypeLikelihoods", ArrayType(DoubleType))
  val alleleDepthsField = StructField("alleleDepths", ArrayType(IntegerType))
  val conditionalQualityField = StructField("conditionalQuality", IntegerType)
  val otherFieldsField = StructField("otherFields", MapType(StringType, StringType))

  // Possible genotype fields for BGEN
  val ploidyField = StructField("ploidy", IntegerType)

  // Genotype fields that are typically present in BGEN records
  def bgenGenotypesField(hasSampleIds: Boolean): StructField = StructField(
    genotypesFieldName,
    ArrayType(
      StructType(
        (if (hasSampleIds) Seq(sampleIdField) else Seq.empty) ++
        Seq(
          phasedField,
          ploidyField,
          posteriorProbabilitiesField
        )
      )
    )
  )

  // All fields that are typically present in BGEN records
  def bgenDefaultSchema(hasSampleIds: Boolean): StructType = StructType(
    Seq(
      contigNameField,
      startField,
      endField,
      namesField,
      refAlleleField,
      alternateAllelesField,
      bgenGenotypesField(hasSampleIds)
    )
  )
}

private[databricks] case class GenotypeFields(
    sampleId: Option[String],
    phased: Option[Boolean],
    calls: Option[Seq[Int]],
    depth: Option[Int],
    filters: Option[Seq[String]],
    genotypeLikelihoods: Option[Seq[Double]],
    phredLikelihoods: Option[Seq[Int]],
    posteriorProbabilities: Option[Seq[Double]],
    conditionalQuality: Option[Int],
    haplotypeQualities: Option[Seq[Int]],
    expectedAlleleCounts: Option[Seq[Int]],
    mappingQuality: Option[Int],
    alleleDepths: Option[Seq[Int]],
    otherFields: scala.collection.Map[String, String])

private[databricks] object GenotypeFields {
  val baseReverseAliases: Map[String, String] = Map(
    "depth" -> "DP",
    "filters" -> "FT",
    "genotypeLikelihoods" -> "GL",
    "phredLikelihoods" -> "PL",
    "posteriorProbabilities" -> "GP",
    "conditionalQuality" -> "GQ",
    "haplotypeQualities" -> "HQ",
    "expectedAlleleCounts" -> "EC",
    "mappingQuality" -> "MQ",
    "alleleDepths" -> "AD"
  )
  val reverseAliases: Map[String, String] = baseReverseAliases ++ Map(
      "calls" -> "GT",
      "phased" -> "GT"
    )

  val aliases: Map[String, Seq[String]] = baseReverseAliases.map { case (k, v) => (v, Seq(k)) } +
    ("GT" -> Seq("phased", "calls"))
}

private[databricks] case class VCFRow(
    contigName: String,
    start: Long,
    end: Long,
    names: Seq[String],
    referenceAllele: String,
    alternateAlleles: Seq[String],
    qual: Option[Double],
    filters: Seq[String],
    attributes: scala.collection.Map[String, String],
    genotypes: Seq[GenotypeFields],
    splitFromMultiAllelic: Boolean = false)

object VCFRow {
  lazy val schema: StructType = ScalaReflection
    .schemaFor[VCFRow]
    .dataType
    .asInstanceOf[StructType]
  lazy val encoder: ExpressionEncoder[VCFRow] = Encoders
    .product[VCFRow]
    .asInstanceOf[ExpressionEncoder[VCFRow]]
}

private[databricks] case class BgenGenotype(
    sampleId: Option[String],
    phased: Option[Boolean],
    ploidy: Option[Int],
    posteriorProbabilities: Seq[Double])

private[databricks] case class BgenRow(
    contigName: String,
    start: Long,
    end: Long,
    names: Seq[String],
    referenceAllele: String,
    alternateAlleles: Seq[String],
    genotypes: Seq[BgenGenotype])

object BgenRow {
  lazy val schema: StructType = ScalaReflection
    .schemaFor[BgenRow]
    .dataType
    .asInstanceOf[StructType]
}
