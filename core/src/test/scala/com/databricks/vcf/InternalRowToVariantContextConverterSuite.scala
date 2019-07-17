package com.databricks.vcf

import scala.collection.JavaConverters._

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFHeader
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.bdgenomics.adam.rdd.VCFMetadataLoader

import com.databricks.hls.sql.HLSBaseTest

class InternalRowToVariantContextConverterSuite extends HLSBaseTest {
  lazy val NA12878 = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val header = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, NA12878)

  private val optionsSeq = Seq(
    Map("flattenInfoFields" -> "true", "includeSampleIds" -> "true"),
    Map("flattenInfoFields" -> "true", "includeSampleIds" -> "false"),
    Map("flattenInfoFields" -> "false", "includeSampleIds" -> "false")
  )

  gridTest("common schema options pass strict validation")(optionsSeq) { options =>
    val df = spark.read.format("com.databricks.vcf").options(options).load(NA12878)
    new InternalRowToVariantContextConverter(
      toggleNullability(df.schema, true),
      header,
      ValidationStringency.STRICT).validate()

    new InternalRowToVariantContextConverter(
      toggleNullability(df.schema, false),
      header,
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
}
