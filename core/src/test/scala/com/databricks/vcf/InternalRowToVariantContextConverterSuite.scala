package com.databricks.vcf

import scala.collection.JavaConverters._

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFHeader
import org.bdgenomics.adam.rdd.VCFMetadataLoader

import com.databricks.hls.sql.HLSBaseTest

class InternalRowToVariantContextConverterSuite extends HLSBaseTest {
  lazy val NA12878 = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val header = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, NA12878)

  private val optionsSeq = Seq(
    Map("flattenInfoFields" -> "true", "includeSampleIds" -> "true"),
    Map("flattenInfoFields" -> "true", "includeSampleIds" -> "false"),
    Map("flattenInfoFields" -> "false", "includeSampleIds" -> "false"))

  gridTest("common schema options pass strict validation")(optionsSeq) { options =>
    val df = spark.read.format("com.databricks.vcf").options(options).load(NA12878)
    new InternalRowToVariantContextConverter(
      df.schema,
      header,
      ValidationStringency.STRICT).validate()
  }
}
