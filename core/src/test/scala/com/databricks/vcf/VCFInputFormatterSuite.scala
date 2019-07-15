package com.databricks.vcf

import java.io.ByteArrayInputStream
import java.nio.charset.Charset

import scala.collection.JavaConverters._

import htsjdk.samtools.seekablestream.ByteArraySeekableStream
import htsjdk.variant.utils.VCFHeaderReader
import htsjdk.variant.vcf.{VCFCompoundHeaderLine, VCFHeader, VCFHeaderLine}
import org.apache.spark.sql.DataFrame
import org.bdgenomics.adam.rdd.VCFMetadataLoader
import org.seqdoop.hadoop_bam.util.WrapSeekable

import com.databricks.hls.sql.HLSBaseTest

class VCFInputFormatterSuite extends HLSBaseTest {
  val vcf = s"$testDataHome/NA12878_21_10002403.vcf"

  private def loadVcf(path: String): DataFrame = {
    spark.read.format("com.databricks.vcf").load(path)
  }

  private def getSchemaLines(header: VCFHeader): Set[VCFCompoundHeaderLine] = {
    header.getInfoHeaderLines.asScala.toSet ++ header.getFormatHeaderLines.asScala.toSet
  }

  private def getAllLines(header: VCFHeader): Set[VCFHeaderLine] = {
    header.getContigLines.asScala.toSet ++
    header.getFilterLines.asScala.toSet ++
    header.getFormatHeaderLines.asScala.toSet ++
    header.getInfoHeaderLines.asScala.toSet ++
    header.getOtherHeaderLines.asScala.toSet
  }

  test("default header") {
    val df = loadVcf(vcf)
    val header = VCFInputFormatter.parseHeader(Map("vcfHeader" -> "default"), df)
    assert(getSchemaLines(header) == VCFRowHeaderLines.allHeaderLines.toSet)
  }

  test("infer header") {
    val df = loadVcf(vcf)
    val header = VCFInputFormatter.parseHeader(Map("vcfHeader" -> "infer"), df)
    assert(getSchemaLines(header) == VCFSchemaInferer.headerLinesFromSchema(df.schema).toSet)
    assert(
      getAllLines(header) !=
      getAllLines(VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, vcf)))
  }

  test("use header path") {
    val df = loadVcf(vcf)
    val header = VCFInputFormatter.parseHeader(Map("vcfHeader" -> vcf), df)
    assert(
      getAllLines(header) ==
      getAllLines(VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, vcf)))
  }

  test("use literal header") {
    val contents =
      """
        |##fileformat=VCFv4.2
        |##FORMAT=<ID=PL,Number=G,Type=Integer,Description="">
        |##contig=<ID=monkey,length=42>
        |##source=DatabricksIsCool
        |""".stripMargin.trim + // write CHROM line by itself to preserve tabs
      "\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNA12878\n"

    val df = loadVcf(vcf)
    val header = VCFInputFormatter.parseHeader(Map("vcfHeader" -> contents), df)
    val parsed = VCFFileWriter.parseHeaderFromString(contents)
    assert(getAllLines(header).nonEmpty)
    assert(getAllLines(header) == getAllLines(parsed))
  }
}
