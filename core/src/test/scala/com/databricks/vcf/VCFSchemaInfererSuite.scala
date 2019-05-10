package com.databricks.vcf

import htsjdk.variant.vcf.{
  VCFFormatHeaderLine,
  VCFHeaderLineCount,
  VCFHeaderLineType,
  VCFInfoHeaderLine
}
import org.apache.spark.sql.types._

import com.databricks.hls.sql.HLSBaseTest

class VCFSchemaInfererSuite extends HLSBaseTest {
  test("includes base fields") {
    val schema = VCFSchemaInferer.inferSchema(false, false, Seq.empty, Seq.empty)
    VariantSchemas.vcfBaseSchema.foreach { field =>
      assert(schema.contains(field))
    }
  }

  test("includes attributes field if not flattening info fields") {
    val schema = VCFSchemaInferer.inferSchema(false, false, Seq.empty, Seq.empty)
    assert(schema.exists(_.name == "attributes"))
  }

  gridTest("sampleId field")(Seq(true, false)) { includeSampleIds =>
    val schema = VCFSchemaInferer.inferSchema(includeSampleIds, false, Seq.empty, Seq.empty)
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
      sqlType: DataType)

  private val infoFields = Seq(
    VCFField(VCFHeaderLineType.Character, None, StringType),
    VCFField(VCFHeaderLineType.String, None, StringType),
    VCFField(VCFHeaderLineType.Integer, None, IntegerType),
    VCFField(VCFHeaderLineType.Float, None, DoubleType),
    // Note: FLAG fields usually have count 0, but for this test it doesn't matter
    VCFField(VCFHeaderLineType.Flag, None, BooleanType),
    VCFField(VCFHeaderLineType.String, Option(VCFHeaderLineCount.G), ArrayType(StringType)),
    VCFField(VCFHeaderLineType.Integer, Option(VCFHeaderLineCount.G), ArrayType(IntegerType)),
    VCFField(VCFHeaderLineType.Float, Option(VCFHeaderLineCount.G), ArrayType(DoubleType))
  )
  private val formatFields = infoFields.filter(_.vcfType != VCFHeaderLineType.Flag)

  gridTest("flatten info field")(infoFields) { field =>
    val infoHeader = field.vcfCount match {
      case Some(t) => new VCFInfoHeaderLine("field", t, field.vcfType, "")
      case None => new VCFInfoHeaderLine("field", 1, field.vcfType, "")
    }
    val schema = VCFSchemaInferer.inferSchema(false, true, Seq(infoHeader), Seq.empty)
    val sqlField = schema.find(_.name == "INFO_field").get
    assert(sqlField.dataType == field.sqlType)
  }

  gridTest("infer format fields")(formatFields) { field =>
    val formatHeader = field.vcfCount match {
      case Some(t) => new VCFFormatHeaderLine("field", t, field.vcfType, "")
      case None => new VCFFormatHeaderLine("field", 1, field.vcfType, "")
    }
    val schema = VCFSchemaInferer.inferSchema(false, false, Seq.empty, Seq(formatHeader))
    val genotypeSchema = schema
      .find(_.name == "genotypes")
      .get
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]
    assert(genotypeSchema.find(_.name == "field").get.dataType == field.sqlType)
  }

  test("validate headers") {
    val field1 = new VCFInfoHeaderLine("f1", 1, VCFHeaderLineType.Integer, "monkey")
    val field2 = new VCFInfoHeaderLine("f1", 1, VCFHeaderLineType.Float, "monkey")
    intercept[IllegalArgumentException] {
      VCFSchemaInferer.inferSchema(false, false, Seq(field1, field2), Seq.empty)
    }
  }
}
