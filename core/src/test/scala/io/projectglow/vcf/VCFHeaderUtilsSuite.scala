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

import java.io.FileNotFoundException
import java.nio.file.Files

import scala.collection.JavaConverters._

import htsjdk.tribble.TribbleException
import htsjdk.variant.vcf._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkException
import org.apache.spark.sql.types.StructType

import io.projectglow.sql.GlowBaseTest

class VCFHeaderUtilsSuite extends GlowBaseTest {
  val vcf = s"$testDataHome/NA12878_21_10002403.vcf"
  lazy val schema: StructType = spark.read.format("vcf").load(vcf).schema
  lazy val vcfMetadataLines: String =
    """
      |##fileformat=VCFv4.2
      |##FORMAT=<ID=PL,Number=G,Type=Integer,Description="">
      |##contig=<ID=monkey,length=42>
      |##source=DatabricksIsCool
      |""".stripMargin.trim // write CHROM line by itself to preserve tabs

  private def getHeaderNoSamples(
      options: Map[String, String],
      defaultOpt: Option[String],
      schema: StructType) = {
    val (headerLines, _) = VCFHeaderUtils.parseHeaderLinesAndSamples(
      options,
      defaultOpt,
      schema,
      spark.sparkContext.hadoopConfiguration)
    new VCFHeader(headerLines.asJava)
  }

  private def getSchemaLines(header: VCFHeader) =
    header.getInfoHeaderLines.asScala.toSet ++ header.getFormatHeaderLines.asScala.toSet

  private def getAllLines(header: VCFHeader) =
    header.getContigLines.asScala.toSet ++
    header.getFilterLines.asScala.toSet ++
    header.getFormatHeaderLines.asScala.toSet ++
    header.getInfoHeaderLines.asScala.toSet ++
    header.getOtherHeaderLines.asScala.toSet

  private def writeVCFHeaders(contents: Seq[String], nSamples: Int = 1): Seq[String] = {
    val dir = Files.createTempDirectory(this.getClass.getSimpleName)
    val paths = contents.indices.map(idx => dir.resolve(idx.toString))
    contents.zip(paths).foreach {
      case (s, path) =>
        val sampleIds = Range(0, nSamples).map(i => s"s$i").mkString("\t")
        val fullContents =
          s"""
           |##fileformat=VCFv4.2
           |${s.trim}
           |#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t$sampleIds
         """.stripMargin
        FileUtils.writeStringToFile(path.toFile, StringContext.treatEscapes(fullContents.trim()))
    }
    paths.map(_.toString)
  }

  test("fall back") {
    val oldHeader = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, vcf)
    val header = getHeaderNoSamples(Map.empty, Some("infer"), schema)
    assert(getSchemaLines(header) == VCFSchemaInferrer.headerLinesFromSchema(schema).toSet)
    assert(getSchemaLines(header) == getSchemaLines(oldHeader))
  }

  test("infer header") {
    val oldHeader = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, vcf)
    val header = getHeaderNoSamples(Map("vcfHeader" -> "infer"), None, schema)
    assert(getSchemaLines(header) == VCFSchemaInferrer.headerLinesFromSchema(schema).toSet)
    assert(getSchemaLines(header) == getSchemaLines(oldHeader))
  }

  test("use header path") {
    val header = getHeaderNoSamples(Map("vcfHeader" -> vcf), None, schema)
    assert(
      getAllLines(header) ==
      getAllLines(VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, vcf)))
  }

  test("use literal header") {
    val contents = vcfMetadataLines + "\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNA12878\n"
    val header = getHeaderNoSamples(Map("vcfHeader" -> contents), None, schema)
    val parsed = VCFHeaderUtils.parseHeaderFromString(contents)
    assert(getAllLines(header).nonEmpty)
    assert(getAllLines(header) == getAllLines(parsed))
  }

  test("throw on bad literal header") {
    // no #CHROM line
    val e =
      intercept[IllegalArgumentException](VCFHeaderUtils.parseHeaderFromString(vcfMetadataLines))
    assert(e.getMessage.contains("Unable to parse VCF header"))
  }

  test("no vcf header arg and no default header") {
    val e = intercept[IllegalArgumentException] {
      getHeaderNoSamples(Map.empty, None, schema)
    }
    assert(e.getMessage.contains("Must specify a method to determine VCF header"))
  }

  test("invalid path") {
    assertThrows[FileNotFoundException] {
      getHeaderNoSamples(Map("vcfHeader" -> "fake.vcf"), None, schema)
    }
  }

  test("invalid VCF") {
    assertThrows[TribbleException] {
      getHeaderNoSamples(Map("vcfHeader" -> s"$testDataHome/no_header.csv"), None, schema)
    }
  }

  test("merge header lines") {
    val file1 =
      s"""
         |##fileformat=VCFv4.2
         |##FORMAT=<ID=AD,Number=R,Type=Integer,Description="">
         |##INFO=<ID=animal,Number=1,Type=String,Description="monkey">
         |##FILTER=<ID=LowQual,Description="Low Quality">
         |##contig=<ID=20,length=63025520>
         |##source=willNotBeIncluded
       """.stripMargin
    val file2 =
      s"""
         |##fileformat=VCFv4.2
         |##FORMAT=<ID=DP,Number=1,Type=Integer,Description="">
         |##INFO=<ID=color,Number=G,Type=String,Description="">
         |##FILTER=<ID=LowQual,Description="Low Quality">
         |##contig=<ID=20,length=63025520>
         |##contig=<ID=21,length=48129895>
       """.stripMargin
    val paths = writeVCFHeaders(Seq(file1, file2))
    val lines = VCFHeaderUtils.readHeaderLines(spark, paths)

    val expectedLines = Set(
      new VCFInfoHeaderLine("animal", 1, VCFHeaderLineType.String, "monkey"),
      new VCFInfoHeaderLine("color", VCFHeaderLineCount.G, VCFHeaderLineType.String, ""),
      new VCFFormatHeaderLine("AD", VCFHeaderLineCount.R, VCFHeaderLineType.Integer, ""),
      new VCFFormatHeaderLine("DP", 1, VCFHeaderLineType.Integer, ""),
      new VCFFilterHeaderLine("LowQual", "Low Quality"),
      new VCFContigHeaderLine("<ID=20,length=63025520>", VCFHeaderVersion.VCF4_2, "contig", 0),
      new VCFContigHeaderLine("<ID=21,length=48129895>", VCFHeaderVersion.VCF4_2, "contig", 1)
    )

    // We compare the string-encoded versions of the header lines to avoid direct object comparisons
    val sortedLines = lines.map(_.toString).toSet
    val sortedExpectedLines = expectedLines.map(_.toString)
    assert(lines.size == sortedLines.size)
    assert(sortedLines == sortedExpectedLines)
  }

  def checkLinesIncompatible(file1: String, file2: String): Unit = {
    val paths = writeVCFHeaders(Seq(file1, file2))
    val ex = intercept[SparkException](VCFHeaderUtils.readHeaderLines(spark, paths))
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
  }

  test("verify that INFO lines are compatible") {
    val file1 =
      s"""
         |##INFO=<ID=animal,Number=1,Type=String,Description="monkey">
       """.stripMargin
    val file2 =
      s"""
         |##INFO=<ID=animal,Number=2,Type=String,Description="monkey">
       """.stripMargin
    checkLinesIncompatible(file1, file2)
  }

  test("verify that FORMAT lines are compatible") {
    val file1 =
      s"""
         |##FORMAT=<ID=animal,Number=1,Type=String,Description="monkey">
       """.stripMargin
    val file2 =
      s"""
         |##FORMAT=<ID=animal,Number=2,Type=String,Description="monkey">
       """.stripMargin
    checkLinesIncompatible(file1, file2)
  }

  test("verify that contig lines are compatible") {
    val file1 =
      s"""
         |##contig=<ID=21,length=48129895>
       """.stripMargin
    val file2 =
      s"""
         |##contig=<ID=21,length=48129896>
       """.stripMargin
    checkLinesIncompatible(file1, file2)
  }

  test("all format and info lines with same id and different types") {
    val file1 =
      s"""
         |##INFO=<ID=animal,Number=1,Type=String,Description="monkey">
       """.stripMargin
    val file2 =
      s"""
         |##FORMAT=<ID=animal,Number=2,Type=String,Description="monkey">
       """.stripMargin
    val paths = writeVCFHeaders(Seq(file1, file2))
    VCFHeaderUtils.readHeaderLines(spark, paths) // no exception
  }
}
