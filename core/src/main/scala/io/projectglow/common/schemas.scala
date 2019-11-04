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

package io.projectglow.common

import org.apache.spark.sql.Encoders
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
  val callsField = StructField("calls", ArrayType(ShortType))
  val depthField = StructField("depth", IntegerType)
  val genotypeFiltersField = StructField("filters", ArrayType(StringType))
  val phredLikelihoodsField = StructField("phredLikelihoods", ArrayType(IntegerType))
  val genotypeLikelihoodsField = StructField("genotypeLikelihoods", ArrayType(DoubleType))
  val alleleDepthsField = StructField("alleleDepths", ArrayType(IntegerType))
  val conditionalQualityField = StructField("conditionalQuality", IntegerType)
  val otherFieldsField = StructField("otherFields", MapType(StringType, StringType))

  // Possible genotype fields for BGEN
  val ploidyField = StructField("ploidy", ShortType)

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

  // Fields for PLINK
  val variantIdField = StructField("variantId", StringType)
  val positionField = StructField("position", DoubleType)
  val alleleOneField = StructField("alleleOne", StringType)
  val alleleTwoField = StructField("alleleTwo", StringType)

  val bimSchema = StructType(
    Seq(
      contigNameField,
      variantIdField,
      positionField,
      startField,
      alleleOneField,
      alleleTwoField
    )
  )

  def plinkGenotypeSchema(hasSampleIds: Boolean): StructField = {
    StructField(
      genotypesFieldName,
      ArrayType(
        StructType(
          (if (hasSampleIds) Seq(sampleIdField) else Seq.empty) :+ callsField
        )
      ))
  }

  val plinkBaseSchema = StructType(
    Seq(
      contigNameField,
      namesField,
      positionField,
      startField,
      endField,
      refAlleleField,
      alternateAllelesField))

  def plinkSchema(hasSampleIds: Boolean): StructType = {
    StructType(plinkBaseSchema :+ plinkGenotypeSchema(hasSampleIds))
  }
}

case class GenotypeFields(
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

object GenotypeFields {
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

case class VCFRow(
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

private[projectglow] case class BgenGenotype(
    sampleId: Option[String],
    phased: Option[Boolean],
    ploidy: Option[Int],
    posteriorProbabilities: Seq[Double])

private[projectglow] case class BgenRow(
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

private[projectglow] case class PlinkGenotype(sampleId: String, calls: Seq[Int])

private[projectglow] case class PlinkRow(
    contigName: String,
    position: Double,
    start: Long,
    end: Long,
    names: Seq[String],
    referenceAllele: String,
    alternateAlleles: Seq[String],
    genotypes: Seq[PlinkGenotype])
