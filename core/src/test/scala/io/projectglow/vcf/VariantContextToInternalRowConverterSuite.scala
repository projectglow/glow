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
import java.lang.{Boolean => JBoolean, Double => JDouble, Integer => JInteger}
import java.util.{ArrayList => JArrayList, HashSet => JHashSet}

import scala.collection.JavaConverters._

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.{Allele, GenotypeBuilder, VariantContextBuilder}
import htsjdk.variant.vcf.{VCFFileReader, VCFHeader, VCFHeaderLine, VCFHeaderLineType, VCFInfoHeaderLine}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.common.{GenotypeFields, VCFRow}

class VariantContextToInternalRowConverterSuite extends VCFConverterBaseTest {

  lazy val NA12878 = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val TGP = s"$testDataHome/1000genomes-phase3-1row.vcf"
  lazy val GVCF = s"$testDataHome/NA12878_21_10002403.g.vcf"

  lazy val defaultHeader = new VCFHeader()
  lazy val lenientConverter = new VariantContextToInternalRowConverter(
    defaultHeader,
    VCFRow.schema,
    ValidationStringency.LENIENT
  )
  lazy val strictConverter = new VariantContextToInternalRowConverter(
    defaultHeader,
    VCFRow.schema,
    ValidationStringency.STRICT
  )

  gridTest("Array fields converted to strings")(Seq(true, false)) { arrToStr =>
    val intArrayHeaderLine =
      new VCFInfoHeaderLine("IntArray", 2, VCFHeaderLineType.Integer, "Integer array")
    val strArrayHeaderLine =
      new VCFInfoHeaderLine("StringArray", 3, VCFHeaderLineType.String, "String array")
    val floatArrayHeaderLine =
      new VCFInfoHeaderLine("FloatArray", 4, VCFHeaderLineType.Float, "Float array")
    val headerLines = Seq(intArrayHeaderLine, strArrayHeaderLine, floatArrayHeaderLine)

    val vcfHeader = new VCFHeader(headerLines.map(_.asInstanceOf[VCFHeaderLine]).toSet.asJava)
    val schema = VCFSchemaInferrer.inferSchema(true, true, vcfHeader)
    val converter =
      new VariantContextToInternalRowConverter(vcfHeader, schema, ValidationStringency.STRICT)

    val vcb = new VariantContextBuilder("Unknown", "chr1", 1, 1, Seq(Allele.REF_A).asJava)
    val intArray = Array(1, 2)
    val strArray = Array("foo", "bar", "baz").map(UTF8String.fromString)
    val floatArray = Array(0.1, 1.2, 2.3, 3.4)

    if (arrToStr) {
      vcb.attribute(intArrayHeaderLine.getID, intArray.mkString(","))
      vcb.attribute(strArrayHeaderLine.getID, strArray.mkString(","))
      vcb.attribute(floatArrayHeaderLine.getID, floatArray.mkString(","))
    } else {
      vcb.attribute(intArrayHeaderLine.getID, intArray)
      vcb.attribute(strArrayHeaderLine.getID, strArray)
      vcb.attribute(floatArrayHeaderLine.getID, floatArray)
    }
    val vc = vcb.make

    val internalRow = converter.convertRow(vc, false)
    assert(
      internalRow
        .getArray(schema.fieldIndex("INFO_" + intArrayHeaderLine.getID))
        .toIntArray() sameElements intArray)
    assert(
      internalRow
        .getArray(schema.fieldIndex("INFO_" + strArrayHeaderLine.getID))
        .toObjectArray(StringType) sameElements strArray)
    assert(
      internalRow
        .getArray(schema.fieldIndex("INFO_" + floatArrayHeaderLine.getID))
        .toDoubleArray() sameElements floatArray)
  }

  def compareVcfRows(vcf: String): Unit = {
    val sess = spark
    import sess.implicits._

    val header = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, vcf)
    val converter =
      new VariantContextToInternalRowConverter(header, VCFRow.schema, ValidationStringency.LENIENT)

    val sparkVcfRowList = spark
      .read
      .format("vcf")
      .schema(VCFRow.schema)
      .load(vcf)
      .as[VCFRow]
      .collect

    val file = new File(vcf)
    val reader = new VCFFileReader(file, false)
    val htsjdkVcList = reader.iterator.toList.asScala
    val htsjdkVcfRowList = convertToVCFRows(htsjdkVcList.map(converter.convertRow(_, false)))

    assert(sparkVcfRowList.length == htsjdkVcfRowList.length)
    sparkVcfRowList.zip(htsjdkVcfRowList).map {
      case (sparkVcfRow, htsjdkVcfRow) =>
        assert(
          sparkVcfRow.copy(qual = None) == htsjdkVcfRow.copy(qual = None),
          s"\nVC1 $sparkVcfRow\nVC2 $htsjdkVcfRow"
        )
        if (sparkVcfRow.qual.isDefined) {
          assert(
            sparkVcfRow.qual.get ~== htsjdkVcfRow.qual.get relTol 0.2,
            s"VC1 qual ${sparkVcfRow.qual.get} VC2 qual ${htsjdkVcfRow.qual.get}"
          )
        }
    }
  }

  test("Single sample") {
    compareVcfRows(NA12878)
  }

  test("Multiple samples") {
    compareVcfRows(TGP)
  }

  test("GVCF") {
    compareVcfRows(GVCF)
  }

  test("Default VariantContext") {
    val vcb = new VariantContextBuilder()
    vcb.chr("")
    vcb.start(1)
    vcb.stop(1)
    val refAllele = Allele.create("A", true)
    vcb.alleles(Seq(refAllele).asJava)
    val defaultVc = vcb.make

    val vcfRow = convertToVCFRow(lenientConverter.convertRow(defaultVc, false))

    val convertedDefaultVc = defaultVcfRow.copy(end = 1, referenceAllele = "A", genotypes = Nil)
    assert(vcfRow == convertedDefaultVc)
  }

  test("Default Genotype") {
    val vcb = new VariantContextBuilder()
    vcb.chr("").start(1).stop(1)
    val refAllele = Allele.create("A", true)
    vcb.alleles(Seq(refAllele).asJava)
    vcb.genotypes(new GenotypeBuilder().make)
    val vcWithDefaultGt = vcb.make.fullyDecode(defaultHeader, true)

    val vcfRow = convertToVCFRow(lenientConverter.convertRow(vcWithDefaultGt, false))

    val convertedVcWithDefaultGt = defaultVcfRow.copy(
      end = 1,
      referenceAllele = "A",
      genotypes = Seq(defaultGenotypeFields.copy(phased = Some(false)))
    )
    assert(vcfRow == convertedVcWithDefaultGt)
  }

  test("Set VariantContext and Genotypes") {
    val refAllele = Allele.create("A", true)
    val altAllele1 = Allele.create("T")
    val altAllele2 = Allele.create("C")

    val gt1Alleles = new JArrayList[Allele]()
    gt1Alleles.add(refAllele)
    gt1Alleles.add(altAllele1)
    val gb1 = new GenotypeBuilder("sample1", gt1Alleles)
    gb1.phased(true)
    gb1.GQ(3)
    gb1.DP(4)
    gb1.AD(Array(10, 11, 12))
    gb1.PL(Array(20, 21, 22, 23, 24, 25, 26))
    val gt1Filters = new JArrayList[String]()
    gt1Filters.add("gtFilter1")
    gt1Filters.add("gtFilter2")
    gb1.filters(gt1Filters)
    gb1.attribute("GP", "2.1,2.2,2.3,2.4,2.5,2.6")
    gb1.attribute("HQ", Array(31, 32))
    val ecArrayList = new JArrayList[JInteger]()
    ecArrayList.add(41)
    ecArrayList.add(42)
    ecArrayList.add(43)
    gb1.attribute("EC", ecArrayList)
    gb1.attribute("MQ", "5")
    gb1.attribute("NullKey", null)
    gb1.attribute("MissingKey", ".")
    gb1.attribute("BoolKey", false)
    gb1.attribute("IntKey", new JInteger(150))
    gb1.attribute("DoubleKey", 1.5)
    gb1.attribute("StringKey", "gtStringVal")
    val gt1 = gb1.make

    val gt2Alleles = new JArrayList[Allele]()
    gt2Alleles.add(altAllele2)
    gt2Alleles.add(Allele.NO_CALL)
    val gt2 = new GenotypeBuilder("sample2", gt2Alleles).make

    val vcAlleles = new JArrayList[Allele]()
    vcAlleles.add(refAllele)
    vcAlleles.add(altAllele1)
    vcAlleles.add(altAllele2)
    val vcb = new VariantContextBuilder("source", "contigName", 101, 101, vcAlleles)
    vcb.log10PError(-1.0).id("id1;id2")
    val vcFilters = new JHashSet[String]()
    vcFilters.add("filter1")
    vcFilters.add("filter2")
    vcb.filters(vcFilters)
    vcb.attribute("NullKey", null)
    vcb.attribute("MissingKey", ".")
    vcb.attribute("BoolKey", true)
    vcb.attribute("IntKey", 50)
    vcb.attribute("DoubleKey", new JDouble(0.5))
    vcb.attribute("StringKey", "stringVal")
    vcb.genotypes(gt1, gt2)

    val vc = vcb.make

    val vcfRow = convertToVCFRow(lenientConverter.convertRow(vc, false))

    val convertedGt1 = GenotypeFields(
      sampleId = Some("sample1"),
      phased = Some(true),
      calls = Some(Seq(0, 1)),
      depth = Some(4),
      filters = Some(Seq("gtFilter1", "gtFilter2")),
      genotypeLikelihoods = None,
      phredLikelihoods = Some(Seq(20, 21, 22, 23, 24, 25, 26)),
      posteriorProbabilities = Some(Seq(2.1, 2.2, 2.3, 2.4, 2.5, 2.6)),
      conditionalQuality = Some(3),
      haplotypeQualities = Some(Seq(31, 32)),
      expectedAlleleCounts = Some(Seq(41, 42, 43)),
      mappingQuality = Some(5),
      alleleDepths = Some(Seq(10, 11, 12)),
      otherFields = Map(
        "IntKey" -> "150",
        "DoubleKey" -> "1.5",
        "MissingKey" -> ".",
        "NullKey" -> ".",
        "StringKey" -> "gtStringVal")
    )
    val convertedGt2 = defaultGenotypeFields.copy(
      sampleId = Some("sample2"),
      phased = Some(false),
      calls = Some(Seq(2, -1))
    )
    val convertedVc = VCFRow(
      contigName = "contigName",
      start = 100,
      end = 101,
      names = Seq("id1", "id2"),
      referenceAllele = "A",
      alternateAlleles = Seq("T", "C"),
      qual = Some(10.0),
      filters = Seq("filter1", "filter2"),
      attributes = Map(
        "NullKey" -> ".",
        "MissingKey" -> ".",
        "BoolKey" -> "",
        "IntKey" -> "50",
        "DoubleKey" -> "0.5",
        "StringKey" -> "stringVal"
      ),
      genotypes = Seq(convertedGt1, convertedGt2)
    )
    assert(vcfRow == convertedVc)
  }

  test("Throw for missing INFO header line with strict validation stringency") {
    val vcb = new VariantContextBuilder()
    vcb.chr("")
    vcb.start(1)
    vcb.stop(1)
    val refAllele = Allele.create("A", true)
    vcb.alleles(Seq(refAllele).asJava)
    vcb.attribute("Key", "Value")
    val vc = vcb.make

    assertThrows[IllegalArgumentException](strictConverter.convertRow(vc, false))
  }

  test("Throw for missing FORMAT header line with strict validation stringency") {
    val vcb = new VariantContextBuilder()
    vcb.chr("")
    vcb.start(1)
    vcb.stop(1)
    val refAllele = Allele.create("A", true)
    vcb.alleles(Seq(refAllele).asJava)
    val gb = new GenotypeBuilder()
    gb.attribute("Key", "Value")
    vcb.genotypes(gb.make)
    val vc = vcb.make

    assertThrows[IllegalArgumentException](strictConverter.convertRow(vc, false))
  }

  val objectsParsedAsStrings: Seq[(AnyRef, String)] = Seq(
    (null, "."),
    ("abc", "abc"),
    (0.5: JDouble, "0.5"),
    (1: JInteger, "1"),
    (true: JBoolean, ""),
    (false: JBoolean, null),
    (new JArrayList[Double](Seq(0.1, 1.2).asJava), "0.1,1.2"),
    (Array(0.2, 2.4, 4.8), "0.2,2.4,4.8")
  )

  gridTest("Convert objects to strings")(objectsParsedAsStrings) {
    case (obj, str) =>
      val parsedObj = VariantContextToInternalRowConverter.parseObjectAsString(obj)
      assert(parsedObj == str)
  }
}
