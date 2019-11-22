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

import scala.collection.JavaConverters._

import htsjdk.variant.vcf.VCFHeader
import org.apache.spark.sql.types.StructType

import io.projectglow.sql.GlowBaseTest

class VCFHeaderUtilsSuite extends GlowBaseTest {
  val vcf = s"$testDataHome/NA12878_21_10002403.vcf"
  lazy val schema = spark.read.format("vcf").load(vcf).schema

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
    val contents =
      """
        |##fileformat=VCFv4.2
        |##FORMAT=<ID=PL,Number=G,Type=Integer,Description="">
        |##contig=<ID=monkey,length=42>
        |##source=DatabricksIsCool
        |""".stripMargin.trim + // write CHROM line by itself to preserve tabs
      "\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNA12878\n"

    val header = getHeaderNoSamples(Map("vcfHeader" -> contents), None, schema)
    val parsed = VCFHeaderUtils.parseHeaderFromString(contents)
    assert(getAllLines(header).nonEmpty)
    assert(getAllLines(header) == getAllLines(parsed))
  }
}
