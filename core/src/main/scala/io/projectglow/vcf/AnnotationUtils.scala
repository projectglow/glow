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

import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StringType, StructField, StructType}

// Unified VCF annotation representation, used by SnpEff and VEP
object AnnotationUtils {

  // Delimiter between annotation fields
  val annotationDelimiter = "|"
  val annotationDelimiterRegex = "\\|"

  // Fractional delimiter for struct subfields
  val structDelimiter = "/"
  val structDelimiterRegex = "\\/"

  // Delimiter for array subfields
  val arrayDelimiter = "&"

  // Struct subfield schemas
  private val rankTotalStruct = StructType(
    Seq(StructField("rank", StringType), StructField("total", StringType)))
  private val posLengthStruct = StructType(
    Seq(StructField("pos", StringType), StructField("length", StringType)))
  private val referenceVariantStruct = StructType(
    Seq(StructField("reference", StringType), StructField("variant", StringType)))

  // Special schemas for SnpEff subfields
  private val snpEffFieldsToSchema: Map[String, DataType] = Map(
    "Annotation" -> ArrayType(StringType),
    "Rank" -> rankTotalStruct,
    "cDNA_pos/cDNA_length" -> posLengthStruct,
    "CDS_pos/CDS_length" -> posLengthStruct,
    "AA_pos/AA_length" -> posLengthStruct,
    "Distance" -> IntegerType
  )

  // Special schemas for VEP subfields
  private val vepFieldsToSchema: Map[String, DataType] = Map(
    "Consequence" -> ArrayType(StringType),
    "EXON" -> rankTotalStruct,
    "INTRON" -> rankTotalStruct,
    "cDNA_position" -> StringType,
    "CDS_position" -> StringType,
    "Protein_position" -> StringType,
    "Amino_acids" -> referenceVariantStruct,
    "Codons" -> referenceVariantStruct,
    "Existing_variation" -> ArrayType(StringType),
    "DISTANCE" -> StringType,
    "STRAND" -> StringType,
    "FLAGS" -> ArrayType(StringType)
  )

  // Special schemas for LOFTEE (as VEP plugin) subfields
  private val lofteeFieldsToSchema: Map[String, DataType] = Map(
    "LoF_filter" -> ArrayType(StringType),
    "LoF_flags" -> ArrayType(StringType),
    "LoF_info" -> ArrayType(StringType)
  )

  // Default string schema for annotation subfield
  val allFieldsToSchema: Map[String, DataType] =
    (snpEffFieldsToSchema ++ vepFieldsToSchema ++ lofteeFieldsToSchema).withDefaultValue(StringType)
}
