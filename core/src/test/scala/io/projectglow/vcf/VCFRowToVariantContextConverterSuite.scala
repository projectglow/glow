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

import scala.collection.JavaConverters._

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.GenotypeLikelihoods
import htsjdk.variant.vcf.{VCFFileReader, VCFHeader}
import org.apache.commons.io.FileUtils

import io.projectglow.common.{GenotypeFields, VCFRow}
import io.projectglow.sql.GlowBaseTest

class VCFRowToVariantContextConverterSuite extends GlowBaseTest with VCFConverterBaseTest {

  lazy val NA12878 = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val TGP = s"$testDataHome/1000genomes-phase3-1row.vcf"
  lazy val GVCF = s"$testDataHome/NA12878_21_10002403.g.vcf"

  lazy val defaultHeader = new VCFHeader(VCFRowHeaderLines.allHeaderLines.toSet.asJava)
  lazy val defaultConverter = new VCFRowToVariantContextConverter(defaultHeader, false)

  lazy val vcfRow: VCFRow = defaultVcfRow.copy(referenceAllele = "A", end = defaultVcfRow.start + 1)

  def writeVcStr(vcStr: String): String = {
    val file = Files.createTempFile("test-vcf", ".vcf")
    FileUtils.writeStringToFile(file.toFile, vcStr)
    file.toString
  }

  def compareVcs(vcf: String): Unit = {
    val sess = spark
    import sess.implicits._

    val header = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, vcf)
    val converter = new VCFRowToVariantContextConverter(header, false)

    val sparkVcfRowList = spark
      .read
      .format("vcf")
      .option("includeSampleIds", true)
      .option("vcfRowSchema", true)
      .load(vcf)
      .as[VCFRow]
      .collect
    val sparkVcList = sparkVcfRowList.map(converter.convert)

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
    val vc = defaultConverter.convert(vcfRow)
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

    val vc = defaultConverter.convert(setVcfRow)
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
    assert(extendedAttributes.get("GP") == "0.1,0.2,0.3,0.4,0.5,0.6")
    assert(extendedAttributes.get("HQ") == "20,21,22,23,24,25,26")
    assert(extendedAttributes.get("EC") == "5,6")
    assert(extendedAttributes.get("MQ") == "7")
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

    val vc = defaultConverter.convert(setVcfRow)

    val glAsPl = GenotypeLikelihoods.fromLog10Likelihoods(gl.toArray).getAsPLs
    val gtSeq = vc.getGenotypesOrderedByName.asScala.toSeq

    assert(gtSeq.head.getPL sameElements glAsPl)
  }

  test("No genotypes") {
    val vc = defaultConverter.convert(vcfRow.copy(genotypes = Nil))
    assert(vc.getGenotypesOrderedByName.asScala.isEmpty)
  }

  test("No GT field") {
    val vc = defaultConverter.convert(vcfRow)

    val gtSeq = vc.getGenotypesOrderedByName.asScala.toSeq
    assert(gtSeq.head.getAlleles.isEmpty)
  }

  test("Throws IllegalArgumentException with no reference allele") {
    assertThrows[IllegalArgumentException](defaultConverter.convert(defaultVcfRow))
  }

  test("Throws ArrayIndexOutOfBoundsException with allele index out of range") {
    val genotypeField = defaultGenotypeFields.copy(calls = Some(Seq(3)))
    val setVcfRow = vcfRow.copy(genotypes = Seq(genotypeField))

    assertThrows[IndexOutOfBoundsException](defaultConverter.convert(setVcfRow))
  }

  test("Throw for missing INFO header line with strict validation stringency") {

    val setVcfRow = vcfRow.copy(attributes = Map("Key" -> "Value"))

    val strictConverter =
      new VCFRowToVariantContextConverter(defaultHeader, false, ValidationStringency.STRICT)
    assertThrows[IllegalArgumentException](strictConverter.convert(setVcfRow))
  }

  test("Throw for missing FORMAT header line with strict validation stringency") {
    val genotypeField = defaultGenotypeFields.copy(otherFields = Map("Key" -> "Value"))
    val setVcfRow = vcfRow.copy(genotypes = Seq(genotypeField))

    val strictConverter =
      new VCFRowToVariantContextConverter(defaultHeader, false, ValidationStringency.STRICT)
    assertThrows[IllegalArgumentException](strictConverter.convert(setVcfRow))
  }

  test("Replace missing sample IDs") {
    val setVcfRow =
      vcfRow.copy(genotypes = Array.fill(3)(defaultGenotypeFields))
    val converter =
      new VCFRowToVariantContextConverter(defaultHeader, true)
    val vc = converter.convert(setVcfRow)
    assert(vc.getGenotypes.asScala.map(_.getSampleName) == Seq("sample_1", "sample_2", "sample_3"))
  }

  test("Do not replace missing sample IDs") {
    val setVcfRow =
      vcfRow.copy(genotypes = Array.fill(3)(defaultGenotypeFields))
    val converter =
      new VCFRowToVariantContextConverter(defaultHeader, false)
    val vc = converter.convert(setVcfRow)
    assert(vc.getGenotypes.asScala.map(_.getSampleName) == Array.fill(3)("").toSeq)
  }
}
