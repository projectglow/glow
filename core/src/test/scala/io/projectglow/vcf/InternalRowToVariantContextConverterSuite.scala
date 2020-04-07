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

import java.util.{List => JList}

import scala.collection.JavaConverters._
import htsjdk.samtools.ValidationStringency
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType, StructField, StructType}
import io.projectglow.sql.GlowBaseTest

class InternalRowToVariantContextConverterSuite extends GlowBaseTest {
  lazy val NA12878 = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val header = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, NA12878)
  lazy val headerLines = header.getMetaDataInInputOrder.asScala.toSet

  private val optionsSeq = Seq(
    Map("flattenInfoFields" -> "true", "includeSampleIds" -> "true"),
    Map("flattenInfoFields" -> "true", "includeSampleIds" -> "false"),
    Map("flattenInfoFields" -> "false", "includeSampleIds" -> "false"),
    Map("splitToBiallelic" -> "true", "includeSampleIds" -> "true")
  )

  gridTest("common schema options pass strict validation")(optionsSeq) { options =>
    val df = spark.read.format("vcf").options(options).load(NA12878)
    new InternalRowToVariantContextConverter(
      toggleNullability(df.schema, true),
      headerLines,
      ValidationStringency.STRICT).validate()

    new InternalRowToVariantContextConverter(
      toggleNullability(df.schema, false),
      headerLines,
      ValidationStringency.STRICT).validate()
  }

  private def toggleNullability[T <: DataType](dt: T, nullable: Boolean): T = dt match {
    case at: ArrayType => at.copy(containsNull = nullable).asInstanceOf[T]
    case st: StructType =>
      val fields = st.map { f =>
        f.copy(dataType = toggleNullability(f.dataType, nullable), nullable = nullable)
      }
      StructType(fields).asInstanceOf[T]
    case mt: MapType => mt.copy(valueContainsNull = nullable).asInstanceOf[T]
    case other => other
  }

  test("find genotype schema") {
    val df = spark.read.format("vcf").load(NA12878)
    val dfSchema = df.schema
    val (header, _) = VCFFileFormat.createVCFCodec(NA12878, spark.sessionState.newHadoopConf())
    val actualGenotypeSchema =
      VCFSchemaInferrer.inferGenotypeSchema(true, header.getFormatHeaderLines.asScala.toSeq)
    assert(
      InternalRowToVariantContextConverter.getGenotypeSchema(dfSchema).get == actualGenotypeSchema)
  }

  test("no genotype schema") {
    val schema = StructType(Seq.empty)
    assert(InternalRowToVariantContextConverter.getGenotypeSchema(schema).isEmpty)
  }

  test("genotype schema is not array") {
    val schema = StructType(Seq(StructField("genotypes", StringType)))
    val e = intercept[IllegalArgumentException] {
      InternalRowToVariantContextConverter.getGenotypeSchema(schema)
    }
    assert(e.getMessage.contains("`genotypes` column must be an array of structs"))
  }

  test("genotype schema of arrays does not contain structs") {
    val schema = StructType(Seq(StructField("genotypes", ArrayType(StringType))))
    val e = intercept[IllegalArgumentException] {
      InternalRowToVariantContextConverter.getGenotypeSchema(schema)
    }
    assert(e.getMessage.contains("`genotypes` column must be an array of structs"))
  }

  test("convert string fields") {
    import org.apache.spark.sql.functions._
    val df = spark
      .range(1)
      .withColumn("contigName", lit("1"))
      .withColumn("start", lit(1))
      .withColumn("end", lit(2))
      .withColumn("referenceAllele", lit("A"))
      .withColumn("alternateAlleles", lit(array(lit("G"))))
      .withColumn("INFO_string", lit("monkey1"))
      .withColumn("INFO_string_array", array(lit("monkey2")))
      .withColumn(
        "genotypes",
        array(struct(lit("monkey3").as("string"), array(lit("monkey4")).as("string_array"))))
    val schema = df.schema
    val vc = df
      .queryExecution
      .toRdd
      .mapPartitions { it =>
        val header = VCFSchemaInferrer.headerLinesFromSchema(schema)
        val converter = new InternalRowToVariantContextConverter(
          schema,
          header.toSet,
          ValidationStringency.STRICT)
        it.flatMap(converter.convert)
      }
      .first()
    assert(vc.getAttribute("string") == "monkey1")
    assert(vc.getAttribute("string_array").asInstanceOf[JList[String]].asScala == Seq("monkey2"))
    val genotype = vc.getGenotype(0)
    assert(genotype.getExtendedAttribute("string") == "monkey3")
    assert(
      genotype.getExtendedAttribute("string_array").asInstanceOf[JList[String]].asScala
      == Seq("monkey4"))
  }
}
