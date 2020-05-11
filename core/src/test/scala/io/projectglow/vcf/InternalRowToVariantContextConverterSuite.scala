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

import java.io.File
import java.nio.file.Files
import java.util.{List => JList}

import scala.collection.JavaConverters._

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.GenotypeLikelihoods
import htsjdk.variant.vcf.VCFFileReader
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StringType, StructField, StructType}

import io.projectglow.common.{GenotypeFields, VCFRow}

class InternalRowToVariantContextConverterSuite extends VCFConverterBaseTest {
  lazy val NA12878 = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val TGP = s"$testDataHome/1000genomes-phase3-1row.vcf"
  lazy val GVCF = s"$testDataHome/NA12878_21_10002403.g.vcf"

  lazy val header = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, NA12878)
  lazy val headerLines = header.getMetaDataInInputOrder.asScala.toSet

  lazy val lenientConverter = new InternalRowToVariantContextConverter(
    VCFRow.schema,
    VCFRowHeaderLines.allHeaderLines.toSet,
    ValidationStringency.LENIENT
  )

  lazy val strictConverter = new InternalRowToVariantContextConverter(
    VCFRow.schema,
    VCFRowHeaderLines.allHeaderLines.toSet,
    ValidationStringency.STRICT
  )

  lazy val vcfRow: VCFRow = defaultVcfRow.copy(referenceAllele = "A", end = defaultVcfRow.start + 1)

  private val optionsSeq = Seq(
    Map("flattenInfoFields" -> "true", "includeSampleIds" -> "true"),
    Map("flattenInfoFields" -> "true", "includeSampleIds" -> "false"),
    Map("flattenInfoFields" -> "false", "includeSampleIds" -> "false")
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

  def writeVcStr(vcStr: String): String = {
    val file = Files.createTempFile("test-vcf", ".vcf")
    FileUtils.writeStringToFile(file.toFile, vcStr)
    file.toString
  }

  def compareVcs(vcf: String): Unit = {
    val sess = spark
    import sess.implicits._

    val header = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, vcf)

    val sparkVcList = spark
      .read
      .format("vcf")
      .schema(VCFRow.schema)
      .load(vcf)
      .sort("contigName", "start", "end")
      .queryExecution
      .toRdd
      .mapPartitions { it =>
        val converter = new InternalRowToVariantContextConverter(
          VCFRow.schema,
          header.getMetaDataInInputOrder.asScala.toSet,
          ValidationStringency.LENIENT
        )
        it.flatMap(converter.convert)
      }
      .collect

    val file = new File(vcf)
    val reader = new VCFFileReader(file, false)
    val htsjdkVcList = reader.iterator.toList.asScala

    assert(htsjdkVcList.length == sparkVcList.length)
    htsjdkVcList.zip(sparkVcList).map {
      case (htsjdkVc, sparkVc) =>
        val htsjdkVcStr = htsjdkVc.fullyDecode(header, false).toString
        val sparkVcStr = sparkVc.fullyDecode(header, false).toString
        assert(htsjdkVcStr == sparkVcStr, s"\nVC1 $htsjdkVcStr\nVC2 $sparkVcStr")
    }

    (sparkVcList, htsjdkVcList, reader.getFileHeader)
  }

  test("Single sample") {
    compareVcs(NA12878)
  }

  test("Multiple samples") {
    compareVcs(TGP)
  }

  test("GVCF") {
    compareVcs(GVCF)
  }

  test("Default VCF row") {
    val vc = lenientConverter.convert(convertToInternalRow(vcfRow)).get
    assert(vc.getContig == defaultVcfRow.contigName)
    assert(vc.getStart == defaultVcfRow.start + 1)
    assert(vc.getEnd == vcfRow.end)
    assert(vc.emptyID)
    assert(vc.getReference.getDisplayString == vcfRow.referenceAllele)
    assert(vc.getAlternateAlleles.isEmpty)
    assert(!vc.hasLog10PError)
    assert(vc.isNotFiltered)
    assert(vc.getAttributes.isEmpty)

    val gt = vc.getGenotypesOrderedByName.asScala.head
    assert(gt.getSampleName == "")
    assert(gt.getAlleles.isEmpty)
    assert(!gt.isPhased)
    assert(!gt.hasDP)
    assert(!gt.isFiltered)
    assert(!gt.hasPL)
    assert(!gt.hasGQ)
    assert(!gt.hasAD)
    assert(gt.getExtendedAttributes.isEmpty)
  }

  test("Set VCF row") {
    val genotypeFields1: GenotypeFields = GenotypeFields(
      sampleId = Some("sample1"),
      phased = Some(true),
      calls = Some(Seq(-1, 0)),
      depth = Some(3),
      filters = Some(Seq("gtFilter1", "gtFilter2")),
      genotypeLikelihoods = Some(Seq(0.11, 0.12, 0.13, 0.14, 0.15, 0.16)),
      phredLikelihoods = Some(Seq(10, 12, 13, 14, 15, 16)),
      posteriorProbabilities = Some(Seq(0.1, 0.2, 0.3, 0.4, 0.5, 0.6)),
      conditionalQuality = Some(4),
      haplotypeQualities = Some(Seq(20, 21, 22, 23, 24, 25, 26)),
      expectedAlleleCounts = Some(Seq(5, 6)),
      mappingQuality = Some(7),
      alleleDepths = Some(Seq(30, 31, 32)),
      otherFields = Map("GT_KEY" -> "val")
    )
    val genotypeFields2 =
      defaultGenotypeFields.copy(
        sampleId = Some("sample2"),
        phased = Some(false),
        calls = Some(Seq(1, 2))
      )

    val setVcfRow = VCFRow(
      contigName = "contigName",
      start = 100,
      end = 200,
      names = Seq("name1", "name2"),
      referenceAllele = "A",
      alternateAlleles = Seq("T", "C"),
      qual = Some(50.0),
      filters = Seq("filter1", "filter2"),
      attributes = Map(
        "END" -> "200",
        "FLAG_WO_HEADER_KEY" -> "",
        "SINGLE_KEY" -> "single",
        "ARRAY_KEY" -> "array1,array2"
      ),
      genotypes = Seq(genotypeFields1, genotypeFields2)
    )

    val vc = lenientConverter.convert(convertToInternalRow(setVcfRow)).get
    assert(vc.getContig == "contigName")
    assert(vc.getStart == 101)
    assert(vc.getEnd == 200)
    assert(vc.getID == "name1;name2")
    assert(vc.getReference.getDisplayString == "A")

    val altAlleles = vc.getAlternateAlleles.asScala
    assert(altAlleles.length == 2)
    assert(altAlleles.head.getDisplayString == "T")
    assert(altAlleles(1).getDisplayString == "C")
    assert(vc.getLog10PError ~== -5 relTol 0.2)

    val filters = vc.getFilters.asScala
    assert(filters.size == 2)
    assert(filters.contains("filter1"))
    assert(filters.contains("filter2"))

    val vcAttributes = vc.getAttributes.asScala
    assert(vcAttributes.size == 4)
    assert(vcAttributes.get("END").contains("200"))
    assert(vcAttributes.get("FLAG_WO_HEADER_KEY").contains("."))
    assert(vcAttributes.get("SINGLE_KEY").contains("single"))
    val arrayVal = vc.getAttributeAsString("ARRAY_KEY", "")
    assert(arrayVal == "array1,array2")

    val gtSeq = vc.getGenotypesOrderedByName.asScala.toSeq
    assert(gtSeq.length == 2)

    val gt1 = gtSeq.head
    assert(gt1.getSampleName == "sample1")

    val alleles1 = gt1.getAlleles.asScala
    assert(alleles1.length == 2)
    assert(alleles1.head.isNoCall)
    assert(alleles1(1).getDisplayString == "A")

    assert(gt1.isPhased)
    assert(gt1.getDP == 3)
    assert(gt1.getFilters == "gtFilter1;gtFilter2")
    assert(gt1.getPL sameElements Array(10, 12, 13, 14, 15, 16))
    assert(gt1.getGQ == 4)
    assert(gt1.getAD sameElements Array(30, 31, 32))

    val extendedAttributes = gt1.getExtendedAttributes
    assert(extendedAttributes.size == 5)
    assert(
      extendedAttributes.get("GP").asInstanceOf[JList[AnyRef]].toArray sameElements Array(0.1, 0.2,
        0.3, 0.4, 0.5, 0.6))
    assert(
      extendedAttributes
        .get("HQ")
        .asInstanceOf[JList[AnyRef]]
        .toArray sameElements Array(20, 21, 22, 23, 24, 25, 26))
    assert(
      extendedAttributes.get("EC").asInstanceOf[JList[AnyRef]].toArray sameElements Array(5, 6))
    assert(extendedAttributes.get("MQ").asInstanceOf[Int] == 7)
    assert(extendedAttributes.get("GT_KEY") == "val")

    val gt2 = gtSeq(1)
    assert(gt2.getSampleName == "sample2")
    val alleles2 = gt2.getAlleles.asScala
    assert(alleles2.length == 2)
    assert(alleles2.head.getDisplayString == "T")
    assert(alleles2(1).getDisplayString == "C")
  }

  test("Set GL field") {
    val gl = Seq(-1.5, -10.5, -100.5, -1000.5, -10000.5, -100000.5)
    val genotypeField = defaultGenotypeFields.copy(genotypeLikelihoods = Some(gl))
    val setVcfRow = vcfRow.copy(genotypes = Seq(genotypeField))

    val vc = lenientConverter.convert(convertToInternalRow(setVcfRow)).get

    val glAsPl = GenotypeLikelihoods.fromLog10Likelihoods(gl.toArray).getAsPLs
    val gtSeq = vc.getGenotypesOrderedByName.asScala.toSeq

    assert(gtSeq.head.getPL sameElements glAsPl)
  }

  test("No genotypes") {
    val vc = lenientConverter.convert(convertToInternalRow(vcfRow.copy(genotypes = Nil))).get
    assert(vc.getGenotypesOrderedByName.asScala.isEmpty)
  }

  test("No GT field") {
    val vc = lenientConverter.convert(convertToInternalRow(vcfRow)).get

    val gtSeq = vc.getGenotypesOrderedByName.asScala.toSeq
    assert(gtSeq.head.getAlleles.isEmpty)
  }

  test("Throws IllegalArgumentException with no reference allele") {
    assertThrows[IllegalArgumentException](
      lenientConverter.convert(convertToInternalRow(defaultVcfRow)))
  }

  test("Throws ArrayIndexOutOfBoundsException with allele index out of range") {
    val genotypeField = defaultGenotypeFields.copy(calls = Some(Seq(3)))
    val setVcfRow = vcfRow.copy(genotypes = Seq(genotypeField))

    assertThrows[IndexOutOfBoundsException](
      lenientConverter.convert(convertToInternalRow(setVcfRow)))
  }

  test("Throw for missing INFO header line with strict validation stringency") {
    val setVcfRow = vcfRow.copy(attributes = Map("Key" -> "Value"))
    assertThrows[IllegalArgumentException](strictConverter.convert(convertToInternalRow(setVcfRow)))
  }

  test("Throw for missing FORMAT header line with strict validation stringency") {
    val genotypeField = defaultGenotypeFields.copy(otherFields = Map("Key" -> "Value"))
    val setVcfRow = vcfRow.copy(genotypes = Seq(genotypeField))

    assertThrows[IllegalArgumentException](strictConverter.convert(convertToInternalRow(setVcfRow)))
  }
}
