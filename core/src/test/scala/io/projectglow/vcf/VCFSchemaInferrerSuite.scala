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

import java.nio.file.Files

import htsjdk.variant.vcf._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import io.projectglow.common.VariantSchemas
import io.projectglow.sql.GlowBaseTest

class VCFSchemaInferrerSuite extends GlowBaseTest {
  test("includes base fields") {
    val schema = VCFSchemaInferrer.inferSchema(false, false, Seq.empty, Seq.empty)
    VariantSchemas.vcfBaseSchema.foreach { field =>
      assert(schema.contains(field))
    }
  }

  test("includes attributes field if not flattening info fields") {
    val schema = VCFSchemaInferrer.inferSchema(false, false, Seq.empty, Seq.empty)
    assert(schema.exists(_.name == "attributes"))
  }

  gridTest("sampleId field")(Seq(true, false)) { includeSampleIds =>
    val schema = VCFSchemaInferrer.inferSchema(includeSampleIds, false, Seq.empty, Seq.empty)
    val genotypesField = schema
      .find(_.name == "genotypes")
      .get
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]
    assert(genotypesField.exists(_.name == "sampleId") == includeSampleIds)
  }

  case class VCFField(
      vcfType: VCFHeaderLineType,
      vcfCount: Option[VCFHeaderLineCount], // None implies count=1
      sqlType: DataType,
      description: String)

  private val infoFields = Seq(
    VCFField(VCFHeaderLineType.Character, None, StringType, "descriptionOne"),
    VCFField(VCFHeaderLineType.String, None, StringType, "descriptionTwo"),
    VCFField(VCFHeaderLineType.Integer, None, IntegerType, "descriptionThree"),
    VCFField(VCFHeaderLineType.Float, None, DoubleType, "descriptionFour"),
    // Note: FLAG fields usually have count 0, but for this test it doesn't matter
    VCFField(VCFHeaderLineType.Flag, None, BooleanType, "descriptionFive"),
    VCFField(
      VCFHeaderLineType.String,
      Option(VCFHeaderLineCount.G),
      ArrayType(StringType),
      "descriptionSix"),
    VCFField(
      VCFHeaderLineType.Integer,
      Option(VCFHeaderLineCount.G),
      ArrayType(IntegerType),
      "descriptionSeven"),
    VCFField(
      VCFHeaderLineType.Float,
      Option(VCFHeaderLineCount.G),
      ArrayType(DoubleType),
      "descriptionEight")
  )
  private val formatFields = infoFields.filter(_.vcfType != VCFHeaderLineType.Flag)

  gridTest("flatten info field")(infoFields) { field =>
    val infoHeader = field.vcfCount match {
      case Some(t) => new VCFInfoHeaderLine("field", t, field.vcfType, field.description)
      case None => new VCFInfoHeaderLine("field", 1, field.vcfType, field.description)
    }
    val schema = VCFSchemaInferrer.inferSchema(false, true, Seq(infoHeader), Seq.empty)
    val sqlField = schema.find(_.name == "INFO_field").get
    assert(sqlField.dataType == field.sqlType)
    assert(sqlField.metadata.contains("vcf_header_description"))
    assert(sqlField.metadata.getString("vcf_header_description") == field.description)
  }

  gridTest("infer format fields")(formatFields) { field =>
    val formatHeader = field.vcfCount match {
      case Some(t) => new VCFFormatHeaderLine("field", t, field.vcfType, field.description)
      case None => new VCFFormatHeaderLine("field", 1, field.vcfType, field.description)
    }
    val schema = VCFSchemaInferrer.inferSchema(false, false, Seq.empty, Seq(formatHeader))
    val genotypeSchema = schema
      .find(_.name == "genotypes")
      .get
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]
    val sqlField = genotypeSchema.find(_.name == "field").get
    assert(sqlField.dataType == field.sqlType)
    assert(sqlField.metadata.contains("vcf_header_description"))
    assert(sqlField.metadata.getString("vcf_header_description") == field.description)
  }

  test("validate headers") {
    val field1 = new VCFInfoHeaderLine("f1", 1, VCFHeaderLineType.Integer, "monkey")
    val field2 = new VCFInfoHeaderLine("f1", 1, VCFHeaderLineType.Float, "monkey")
    intercept[IllegalArgumentException] {
      VCFSchemaInferrer.inferSchema(false, false, Seq(field1, field2), Seq.empty)
    }
  }

  case class ToFromSchemaTestCase(
      infoHeaderLines: Seq[VCFInfoHeaderLine],
      formatHeaderLines: Seq[VCFFormatHeaderLine])
  private val cases = Seq(
    ToFromSchemaTestCase(
      Seq(new VCFInfoHeaderLine("a", 14, VCFHeaderLineType.String, "")),
      Seq(new VCFFormatHeaderLine("b", 1, VCFHeaderLineType.String, ""))
    ),
    ToFromSchemaTestCase(
      Seq(new VCFInfoHeaderLine("a", VCFHeaderLineCount.A, VCFHeaderLineType.String, "")),
      Seq(new VCFFormatHeaderLine("a", VCFHeaderLineCount.A, VCFHeaderLineType.String, ""))
    ),
    ToFromSchemaTestCase(
      Seq(new VCFInfoHeaderLine("a", VCFHeaderLineCount.UNBOUNDED, VCFHeaderLineType.Integer, "")),
      Seq(new VCFFormatHeaderLine("a", VCFHeaderLineCount.UNBOUNDED, VCFHeaderLineType.Integer, ""))
    ),
    ToFromSchemaTestCase(
      Seq(new VCFInfoHeaderLine("monkey", 0, VCFHeaderLineType.Flag, "")),
      Seq()
    ),
    ToFromSchemaTestCase(
      // Field that has pretty name in genotype schema
      Seq(new VCFInfoHeaderLine("PL", 0, VCFHeaderLineType.Flag, "")),
      Seq()
    ),
    ToFromSchemaTestCase(
      Seq.empty,
      Seq.empty
    ),
    ToFromSchemaTestCase(
      Seq(new VCFInfoHeaderLine("a", VCFHeaderLineCount.G, VCFHeaderLineType.Float, "")),
      Seq(new VCFFormatHeaderLine("b", VCFHeaderLineCount.R, VCFHeaderLineType.Float, ""))
    )
  )

  gridTest("to and from schema")(cases) { tc =>
    val schema = VCFSchemaInferrer.inferSchema(true, true, tc.infoHeaderLines, tc.formatHeaderLines)
    val lines = VCFSchemaInferrer.headerLinesFromSchema(schema)
    val allInputLines: Seq[VCFHeaderLine] = tc.formatHeaderLines ++ tc.infoHeaderLines
    assert(lines.toSet == allInputLines.toSet)
  }

  test("include count metadata (non-integer)") {
    val line = new VCFInfoHeaderLine("a", VCFHeaderLineCount.A, VCFHeaderLineType.Integer, "")
    val schema = VCFSchemaInferrer.inferSchema(true, true, Seq(line), Seq.empty)
    assert(schema.exists { f =>
      f.name == "INFO_a" && f.metadata.getString(VCFSchemaInferrer.VCF_HEADER_COUNT_KEY) == "A"
    })
  }

  test("include count metadata (integer") {
    val line = new VCFInfoHeaderLine("a", 102, VCFHeaderLineType.Integer, "")
    val schema = VCFSchemaInferrer.inferSchema(true, true, Seq(line), Seq.empty)
    assert(schema.exists { f =>
      f.name == "INFO_a" && f.metadata.getString(VCFSchemaInferrer.VCF_HEADER_COUNT_KEY) == "102"
    })
  }

  test("counts for fields without metadata") {
    val schema = StructType(
      Seq(
        StructField("INFO_a", IntegerType),
        StructField("INFO_b", BooleanType),
        StructField("INFO_c", ArrayType(IntegerType))))
    val expected = Seq(
      new VCFInfoHeaderLine("a", 1, VCFHeaderLineType.Integer, ""),
      new VCFInfoHeaderLine("b", 0, VCFHeaderLineType.Flag, ""),
      new VCFInfoHeaderLine("c", VCFHeaderLineCount.UNBOUNDED, VCFHeaderLineType.Integer, "")
    )
    assert(VCFSchemaInferrer.headerLinesFromSchema(schema) == expected)
  }

  test("don't include sample ids or otherFields") {
    val schema = StructType(
      Seq(
        StructField(
          "genotypes",
          ArrayType(
            StructType(
              Seq(
                StructField("sampleId", StringType),
                StructField("otherFields", MapType(StringType, StringType))
              ))))))
    assert(VCFSchemaInferrer.headerLinesFromSchema(schema).isEmpty)
  }

  test("don't return same key multiple times") {
    val schema = StructType(
      Seq(
        StructField(
          "genotypes",
          ArrayType(
            StructType(
              Seq(
                StructField("calls", ArrayType(IntegerType)),
                StructField("phased", BooleanType)
              ))))))
    val expected = Seq(new VCFFormatHeaderLine("GT", 1, VCFHeaderLineType.String, "Genotype"))
    assert(VCFSchemaInferrer.headerLinesFromSchema(schema) == expected)
  }

  // Will fail if any column names cannot be saved safely
  def checkSave(schema: StructType): Unit = {
    spark
      .createDataFrame(sparkContext.emptyRDD[Row], schema)
      .write
      .parquet(Files.createTempDirectory("schemaInferrerSuite").resolve("temp").toString)
  }

  test("CSQ") {
    val description =
      "Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|" +
      "Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|" +
      "Existing_variation|DISTANCE|STRAND|FLAGS|SYMBOL_SOURCE|HGNC_ID|LoF|LoF_filter|LoF_flags|LoF_info"
    val csqHeaderLine =
      new VCFInfoHeaderLine(
        "CSQ",
        VCFHeaderLineCount.UNBOUNDED,
        VCFHeaderLineType.String,
        description)

    val inferredSchema = VCFSchemaInferrer.inferSchema(true, true, Seq(csqHeaderLine), Seq.empty)
    val csqField = inferredSchema.fields.find(_.name == "INFO_CSQ").get
    assert(csqField.metadata.getString("vcf_header_count") == "UNBOUNDED")
    assert(csqField.metadata.getString("vcf_header_description") == description)
    assert(
      csqField.dataType ==
      ArrayType(StructType(Seq(
        StructField("Allele", StringType),
        StructField("Consequence", ArrayType(StringType)),
        StructField("IMPACT", StringType),
        StructField("SYMBOL", StringType),
        StructField("Gene", StringType),
        StructField("Feature_type", StringType),
        StructField("Feature", StringType),
        StructField("BIOTYPE", StringType),
        StructField(
          "EXON",
          StructType(Seq(StructField("rank", StringType), StructField("total", StringType)))),
        StructField(
          "INTRON",
          StructType(Seq(StructField("rank", StringType), StructField("total", StringType)))),
        StructField("HGVSc", StringType),
        StructField("HGVSp", StringType),
        StructField("cDNA_position", StringType),
        StructField("CDS_position", StringType),
        StructField("Protein_position", StringType),
        StructField(
          "Amino_acids",
          StructType(
            Seq(StructField("reference", StringType), StructField("variant", StringType)))),
        StructField(
          "Codons",
          StructType(
            Seq(StructField("reference", StringType), StructField("variant", StringType)))),
        StructField("Existing_variation", ArrayType(StringType)),
        StructField("DISTANCE", StringType),
        StructField("STRAND", StringType),
        StructField("FLAGS", ArrayType(StringType)),
        StructField("SYMBOL_SOURCE", StringType),
        StructField("HGNC_ID", StringType),
        StructField("LoF", StringType),
        StructField("LoF_filter", ArrayType(StringType)),
        StructField("LoF_flags", ArrayType(StringType)),
        StructField("LoF_info", ArrayType(StringType))
      ))))

    val inferredHeaderLines = VCFSchemaInferrer.headerLinesFromSchema(inferredSchema)
    assert(inferredHeaderLines.length == 1)
    assert(inferredHeaderLines.head == csqHeaderLine)

    checkSave(inferredSchema)
  }

  test("ANN") {
    val description =
      "Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | " +
      "Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | " +
      "AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO' "
    val annHeaderLine =
      new VCFInfoHeaderLine(
        "ANN",
        VCFHeaderLineCount.UNBOUNDED,
        VCFHeaderLineType.String,
        description)

    val inferredSchema = VCFSchemaInferrer.inferSchema(true, true, Seq(annHeaderLine), Seq.empty)
    val annField = inferredSchema.fields.find(_.name == "INFO_ANN").get
    assert(annField.metadata.getString("vcf_header_count") == "UNBOUNDED")
    assert(annField.metadata.getString("vcf_header_description") == description)
    assert(
      annField.dataType ==
      ArrayType(StructType(Seq(
        StructField("Allele", StringType),
        StructField("Annotation", ArrayType(StringType)),
        StructField("Annotation_Impact", StringType),
        StructField("Gene_Name", StringType),
        StructField("Gene_ID", StringType),
        StructField("Feature_Type", StringType),
        StructField("Feature_ID", StringType),
        StructField("Transcript_BioType", StringType),
        StructField(
          "Rank",
          StructType(Seq(StructField("rank", StringType), StructField("total", StringType)))),
        StructField("HGVS_c", StringType),
        StructField("HGVS_p", StringType),
        StructField(
          "cDNA_pos/cDNA_length",
          StructType(Seq(StructField("pos", StringType), StructField("length", StringType)))),
        StructField(
          "CDS_pos/CDS_length",
          StructType(Seq(StructField("pos", StringType), StructField("length", StringType)))),
        StructField(
          "AA_pos/AA_length",
          StructType(Seq(StructField("pos", StringType), StructField("length", StringType)))),
        StructField("Distance", IntegerType),
        StructField("ERRORS/WARNINGS/INFO", StringType)
      ))))

    val inferredHeaderLines = VCFSchemaInferrer.headerLinesFromSchema(inferredSchema)
    assert(inferredHeaderLines.length == 1)
    assert(inferredHeaderLines.head == annHeaderLine)

    checkSave(inferredSchema)
  }
}
