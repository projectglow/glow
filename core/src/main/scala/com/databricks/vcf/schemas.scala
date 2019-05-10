package com.databricks.vcf

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._

object VariantSchemas {
  // Default fields common to VCF and BGEN
  val contigNameField = StructField("contigName", StringType)
  val startField = StructField("start", LongType)
  val endField = StructField("end", LongType)
  val namesField = StructField("names", ArrayType(StringType))
  val refAlleleField = StructField("referenceAllele", StringType)
  val alternateAllelesField = StructField("alternateAlleles", ArrayType(StringType))
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

  // Possible genotype fields
  val sampleIdField = StructField("sampleId", StringType)
  val genotypeField = StructField("genotype", Genotype.schema)
  val posteriorProbabilitiesField = StructField("posteriorProbabilities", ArrayType(DoubleType))
  val depthField = StructField("depth", IntegerType)
  val genotypeFiltersField = StructField("filters", ArrayType(StringType))
  val phredLikelihoodsField = StructField("phredLikelihoods", ArrayType(IntegerType))
  val genotypeLikelihoodsField = StructField("genotypeLikelihoods", ArrayType(DoubleType))
  val alleleDepthsField = StructField("alleleDepths", ArrayType(IntegerType))
  val conditionalQualityField = StructField("conditionalQuality", IntegerType)
  val otherFieldsField = StructField("otherFields", MapType(StringType, StringType))

  // Genotype fields that are typically present in BGEN records
  val bgenGenotypesField = StructField(
    genotypesFieldName,
    ArrayType(
      StructType(
        Seq(
          sampleIdField,
          posteriorProbabilitiesField
        )
      )
    )
  )

  // All fields that are typically present in BGEN records
  val bgenDefaultSchema = StructType(
    Seq(
      contigNameField,
      startField,
      endField,
      namesField,
      refAlleleField,
      alternateAllelesField,
      bgenGenotypesField
    )
  )
}

private[databricks] case class Genotype(calls: Seq[Int], phased: Boolean)

object Genotype {
  lazy val schema: StructType = StructType(
    Seq(
      StructField("calls", ArrayType(IntegerType)),
      StructField("phased", BooleanType)
    )
  )
}

private[databricks] case class GenotypeFields(
    sampleId: Option[String],
    genotype: Option[Genotype],
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
  val reverseAliases: Map[String, String] = Map(
    "genotype" -> "GT",
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
  val aliases: Map[String, String] = reverseAliases.map { case (k, v) => (v, k) }
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
    posteriorProbabilities: Seq[Double])

private[databricks] case class BgenRow(
    contigName: String,
    start: Long,
    end: Long,
    names: Seq[String],
    referenceAllele: String,
    alternateAlleles: Seq[String],
    genotypes: Seq[BgenGenotype])
