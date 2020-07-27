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

import java.nio.file.Files

import htsjdk.variant.vcf.VCFConstants
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.{SparkConf, SparkException}

import io.projectglow.common.{GenotypeFields, VCFRow}
import io.projectglow.sql.GlowBaseTest

class VCFDatasourceSuite extends GlowBaseTest {

  val sourceName = "vcf"

  lazy val testVcf = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val multiAllelicVcf = s"$testDataHome/combined.chr20_18210071_18210093.g.vcf"
  lazy val tgpVcf = s"$testDataHome/1000genomes-phase3-1row.vcf"
  lazy val stringInfoFieldsVcf = s"$testDataHome/test.chr17.vcf"

  override def sparkConf: SparkConf = {
    super
      .sparkConf
      .set("spark.hadoop.io.compression.codecs", "org.seqdoop.hadoop_bam.util.BGZFCodec")
  }

  def makeVcfRow(strSeq: Seq[String]): String = {
    (Seq("1", "1", "id", "C", "T,GT", "1", ".") ++ strSeq).mkString("\t")
  }

  test("default schema") {
    val df = spark.read.format(sourceName).load(testVcf)
    assert(df.schema.exists(_.name.startsWith("INFO_")))
    assert(df.where(expr("size(filter(genotypes, g -> g.sampleId is null)) > 0")).count() == 0)
  }

  test("parse VCF") {
    val datasource = spark.read.format(sourceName).load(testVcf)
    datasource.count()
  }

  test("no sample ids") {
    val schema = spark
      .read
      .format(sourceName)
      .option("includeSampleIds", false)
      .load(testVcf)
      .withColumn("g", expr("genotypes[0]"))
      .selectExpr("g.*")
      .schema

    assert(!schema.exists(_.name == "sampleId"))
  }

  test("with sample ids") {
    val datasource = spark
      .read
      .format(sourceName)
      .load(testVcf)
    val size = datasource.count()
    assert(datasource.where("genotypes[0].sampleId = 'NA12878'").count() == size)
  }

  test("check parsed row") {
    val sess = spark
    import sess.implicits._
    val datasource = spark
      .read
      .format(sourceName)
      .schema(VCFRow.schema)
      .load(testVcf)
    val expected = VCFRow(
      "20",
      9999995,
      9999996,
      Seq.empty,
      "A",
      Seq("ACT"),
      Some(3775.73),
      Seq.empty,
      Map(
        "AC" -> "2",
        "AF" -> "1.00",
        "AN" -> "2",
        "DP" -> "84",
        "ExcessHet" -> "3.0103",
        "FS" -> "0.000",
        "MLEAC" -> "2",
        "MLEAF" -> "1.00",
        "MQ" -> "60.44",
        "QD" -> "25.36",
        "SOR" -> "1.075"
      ),
      Seq(
        GenotypeFields(
          Some("NA12878"),
          Some(false),
          Some(Seq(1, 1)),
          Option(84),
          None,
          None,
          Some(Seq(3813, 256, 0)),
          None,
          Option(99),
          None,
          None,
          None,
          Some(Seq(0, 84)),
          Map.empty
        )
      ),
      false
    )

    compareRows(datasource.orderBy("contigName", "start").as[VCFRow].head(), expected)
  }

  test("multiple genotype fields") {
    val sess = spark
    import sess.implicits._
    val input = s"$testDataHome/1000genomes-phase3-1row.vcf"
    val df = spark.read.format(sourceName).load(input)
    assert(df.selectExpr("size(genotypes)").as[Int].head == 2504)
  }

  test("filter with and without pushdown returns same results") {
    val sess = spark
    import sess.implicits._

    def checkResultsMatch(f: DataFrame => Long): Unit = {
      val withPushdown = spark
        .read
        .format(sourceName)
        .option("enablePredicatePushdown", true)
        .load(testVcf)
      val withoutPushdown = spark
        .read
        .format(sourceName)
        .option("enablePredicatePushdown", false)
        .load(testVcf)
      assert(f(withPushdown) == f(withoutPushdown))
    }

    checkResultsMatch(_.count())

    checkResultsMatch { df =>
      df.where(expr("qual > 1"))
        .agg(expr("max(size(genotypes))"))
        .as[Long]
        .head
    }

    checkResultsMatch { df =>
      df.where(expr("size(filter(genotypes, g -> g.calls[0] = 0)) > 50")).count()
    }
  }

  protected def parseVcfContents(row: String, nSamples: Int = 1): Dataset[VCFRow] = {
    val sess = spark
    import sess.implicits._
    val file = Files.createTempFile("test-vcf", ".vcf")
    val samples = (1 to nSamples).map(n => s"sample_$n").mkString("\t")
    val headers =
      s"##fileformat=VCFv4.2\n" +
      s"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t$samples\n"
    FileUtils.writeStringToFile(file.toFile, headers + row)
    spark
      .read
      .format(sourceName)
      .schema(VCFRow.schema)
      .load(file.toString)
      .as[VCFRow]
  }

  test("uncalled genotype") {
    val row = parseVcfContents(makeVcfRow(Seq("AC=2", "GT", "."))).head
    assert(row.genotypes.head.calls.get == Seq(-1))
  }

  test("split uncalled genotype") {
    val row = parseVcfContents(makeVcfRow(Seq("AC=2", "GT", "./."))).head
    assert(row.genotypes.head.calls.get == Seq(-1, -1))
  }

  test("phased genotype") {
    val row = parseVcfContents(makeVcfRow(Seq("AC=2", "GT", "0|0"))).head
    val genotype = row.genotypes.head
    assert(genotype.calls.get == Seq(0, 0))
    assert(genotype.phased.get)
  }

  test("unphased genotype") {
    val row = parseVcfContents(makeVcfRow(Seq("AC=2", "GT", "0/0"))).head
    val genotype = row.genotypes.head
    assert(genotype.calls.get == Seq(0, 0))
    assert(!genotype.phased.get)
  }

  test("format field flags") {
    val row = parseVcfContents(makeVcfRow(Seq("DB", "GT", "1|2"))).head
    val attributes = row.attributes
    assert(attributes.size == 1)
    assert(attributes.get("DB").contains(""))
  }

  test("missing info values") {
    val row = parseVcfContents(makeVcfRow(Seq("AC=2", "GT:MIN_DP:SB", "1|2:.:3,4,5,6"))).head
    val otherFields = row.genotypes.head.otherFields
    assert(otherFields.size == 1)
    assert(!otherFields.contains("MIN_DP"))
  }

  test("missing format values") {
    val row = parseVcfContents(
      makeVcfRow(Seq("AC=2", "GT:DP:FT:GL:PL:GP:GQ:HQ:EC:MQ:AD", ".:.:.:.:.:.:.:.:.:.:."))
    ).head
    val gt = row.genotypes.head
    assert(gt.depth.isEmpty)
    assert(gt.filters.isEmpty)
    assert(gt.genotypeLikelihoods.isEmpty)
    assert(gt.phredLikelihoods.isEmpty)
    assert(gt.posteriorProbabilities.isEmpty)
    assert(gt.conditionalQuality.isEmpty)
    assert(gt.haplotypeQualities.isEmpty)
    assert(gt.expectedAlleleCounts.isEmpty)
    assert(gt.mappingQuality.isEmpty)
    assert(gt.alleleDepths.isEmpty)
  }

  test("dropped trailing format values") {
    val row = parseVcfContents(
      makeVcfRow(Seq("AC=2", "GT:MIN_DP:DP:FT:GL:PL:GP:GQ:HQ:EC:MQ:AD:SB", ".:5"))
    ).head
    val gt = row.genotypes.head
    assert(gt.depth.isEmpty)
    assert(gt.filters.isEmpty)
    assert(gt.genotypeLikelihoods.isEmpty)
    assert(gt.phredLikelihoods.isEmpty)
    assert(gt.posteriorProbabilities.isEmpty)
    assert(gt.conditionalQuality.isEmpty)
    assert(gt.haplotypeQualities.isEmpty)
    assert(gt.expectedAlleleCounts.isEmpty)
    assert(gt.mappingQuality.isEmpty)
    assert(gt.alleleDepths.isEmpty)
    assert(gt.otherFields.size == 1)
    assert(!gt.otherFields.contains("SB"))
  }

  test("missing GT format field") {
    val row = parseVcfContents(makeVcfRow(Seq(".", "GL", "."))).head
    val gt = row.genotypes.head
    assert(gt.phased.contains(false)) // By default, HTSJDK parses VCs as unphased
    assert(gt.calls.isEmpty)
  }

  test("missing calls are -1 (zero present") {
    val row = parseVcfContents(makeVcfRow(Seq(".", "GT", "./."))).head
    val gt = row.genotypes.head
    assert(gt.calls.get == Seq(-1, -1))
    assert(gt.phased.contains(false))
  }

  test("missing calls are -1 (only one present)") {
    val row = parseVcfContents(makeVcfRow(Seq(".", "GT", "1|."))).head
    val gt = row.genotypes.head
    assert(gt.calls.get == Seq(1, -1))
    assert(gt.phased.contains(true))
  }

  test("set END field") {
    val row = parseVcfContents(makeVcfRow(Seq("END=200", "GT", "."))).head
    assert(row.end == 200)
  }

  test("read VCFv4.3") {
    val input = spark
      .read
      .format(sourceName)
      .load(s"$testDataHome/vcf/VCFv4.3.vcf")

    assert(input.count == 5)
  }

  test("splitToBiallelic option error message") {
    val ds = spark
      .read
      .format(sourceName)
      .option("splitToBiallelic", true)
      .load(multiAllelicVcf)
    val e = intercept[IllegalArgumentException] {
      ds.collect()
    }
    assert(e.getMessage.contains("split_multiallelics transformer"))
  }

  test("strict validation stringency") {
    val row = makeVcfRow(Seq("AC=monkey"))

    val file = Files.createTempFile("test-vcf", ".vcf")
    val headers =
      s"""##fileformat=VCFv4.2
         |##INFO=<ID=AC,Number=1,Type=Integer,Description="">
         |#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
        """.stripMargin
    FileUtils.writeStringToFile(file.toFile, headers + row)

    val ds = spark
      .read
      .format(sourceName)
      .option("validationStringency", "strict")
      .option("flattenInfoFields", true)
      .load(file.toString)
    assertThrows[SparkException](ds.collect)
  }

  test("invalid validation stringency") {
    assertThrows[IllegalArgumentException] {
      spark
        .read
        .format(sourceName)
        .option("validationStringency", "fakeStringency")
        .load(testVcf)
    }
  }

  protected def parseDF(
      row: String,
      nSamples: Int = 1,
      extraHeaderLines: String = ""): DataFrame = {
    val file = Files.createTempFile("test-vcf", ".vcf")
    val samples = (1 to nSamples).map(n => s"sample_$n").mkString("\t")
    val headers =
      s"##fileformat=VCFv4.2\n" + extraHeaderLines +
      s"#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t$samples\n"
    val rowStr = headers + row
    FileUtils.writeStringToFile(file.toFile, rowStr)
    spark
      .read
      .format(sourceName)
      .load(file.toString)
  }

  test("flatten INFO fields") {
    val sess = spark
    import sess.implicits._

    val ds = spark
      .read
      .format(sourceName)
      .option("flattenInfoFields", true)
      .load(testVcf)
    val vc = ds
      .orderBy("contigName", "start")
      .select(ds.colRegex("`INFO.*`"))
      .as[INFOFields]
      .head

    assert(vc.INFO_AC == Seq(Some(2)))
    assert(vc.INFO_AF == Seq(Some(1.00)))
    assert(vc.INFO_AN.contains(2))
    assert(vc.INFO_BaseQRankSum.isEmpty)
    assert(vc.INFO_DP.contains(84))
    assert(vc.INFO_DS.isEmpty)
    assert(vc.INFO_ExcessHet.get ~== 3.0103 relTol 0.2)
    assert(vc.INFO_FS.get ~== 0.000 relTol 0.2)
    assert(vc.INFO_MLEAC == Seq(Some(2)))
    assert(vc.INFO_MLEAF.head.get ~== 1.00 relTol 0.2)
    assert(vc.INFO_MQ.get ~== 60.44 relTol 0.2)
    assert(vc.INFO_MQRankSum.isEmpty)
    assert(vc.INFO_QD.get ~== 25.36 relTol 0.2)
    assert(vc.INFO_ReadPosRankSum.isEmpty)
    assert(vc.INFO_SOR.get ~== 1.075 relTol 0.2)
  }

  test("flattened INFO fields schema does not include END key") {
    val schema = spark
      .read
      .format(sourceName)
      .option("flattenInfoFields", true)
      .load(tgpVcf)
      .schema
    assert(!schema.fieldNames.contains(VCFConstants.END_KEY))
  }

  test("flattened INFO fields schema merged for multiple files") {
    val schema = spark
      .read
      .format(sourceName)
      .option("flattenInfoFields", true)
      .load(testVcf, tgpVcf)
      .schema
    assert(schema.fieldNames.length == 49)
    assert(schema.fieldNames.contains("INFO_MQRankSum")) // only in CEUTrio
    assert(schema.fieldNames.contains("INFO_EX_TARGET")) // only in 1KG
  }

  test("prune a flattened INFO field") {
    val sess = spark
    import sess.implicits._

    val dpDf = spark
      .read
      .format(sourceName)
      .option("flattenInfoFields", true)
      .load(testVcf)
      .select(avg("INFO_DP"))
      .as[Double]
    assert(dpDf.head ~== 75.50232558139535 relTol 0.2)
  }

  test("parse string INFO fields") {
    val sess = spark
    import sess.implicits._

    val platformNamesDf = spark
      .read
      .format(sourceName)
      .option("flattenInfoFields", true)
      .load(stringInfoFieldsVcf)
      .orderBy("contigName", "start")
      .select("INFO_platformnames")
      .as[Seq[String]]

    assert(platformNamesDf.head == Seq("Illumina", "CG", "10X", "Solid"))
  }

  test("partitioned file without all of header") {
    // The real header is >7KB, so this contains a truncated header
    val partitionedFile = PartitionedFile(InternalRow(), testVcf, 0L, 6000L)
    val vcfFileFormat = new VCFFileFormat()

    val rowIter = vcfFileFormat.buildReader(
      spark,
      StructType(Seq(StructField("value", StringType))),
      StructType(Seq.empty),
      ScalaReflection.schemaFor[VCFRow].dataType.asInstanceOf[StructType],
      Seq.empty,
      Map("validationStringency" -> "SILENT"),
      spark.sparkContext.hadoopConfiguration
    )(partitionedFile)
    assert(rowIter.isEmpty)
  }

  test("gzip splits are only read if they contain the beginning of the file") {
    val path = s"$testDataHome/vcf/1row_not_bgz.vcf.gz"
    val key = "spark.sql.files.maxPartitionBytes"
    val conf = Map("spark.sql.files.maxPartitionBytes" -> "10")
    withSparkConf(conf) {
      assert(spark.read.format(sourceName).load(path).count() == 1)
    }
  }

  test("misnumbered fields") {
    val rows = spark
      .read
      .format(sourceName)
      .load(s"$testDataHome/vcf/misnumbered_info.vcf")
      .rdd
      .count()
    assert(rows == 1)
  }

  test("multiple rows") {
    spark
      .read
      .format(sourceName)
      .load(testVcf)
      .collect() // Should not get an error
  }

  test("infer non standard format fields") {
    val sess = spark
    import sess.implicits._

    val headerLines = s"""##FORMAT=<ID=MONKEY,Number=1,Type=String,Description="">
                         |##FORMAT=<ID=NUMBERS,Number=5,Type=Float,Description="">
                         |""".stripMargin
    val rowStr = makeVcfRow(Seq(".", "MONKEY:NUMBERS", "banana:1,2,3"))
    val value = parseDF(rowStr, extraHeaderLines = headerLines)
      .selectExpr("genotypes[0].MONKEY")
      .as[String]
      .head
    assert(value == "banana")

    val value2 = parseDF(rowStr, extraHeaderLines = headerLines)
      .selectExpr("genotypes[0].NUMBERS")
      .as[Seq[Double]]
      .head
    assert(value2 == Seq(1, 2, 3))
  }

  case class WeirdSchema(animal: String)
  test("be permissive if schema includes fields that can't be derived from VCF") {
    spark
      .read
      .schema(ScalaReflection.schemaFor[WeirdSchema].dataType.asInstanceOf[StructType])
      .format(sourceName)
      .load(testVcf)
      .collect() // No error expected
  }

  test("add BGZ codec when reading VCF") {
    val gzPath = new Path(s"$testDataHome/vcf/1row_bgz.vcf.gz")
    val bgzPath = new Path(s"$testDataHome/vcf/1row.vcf.bgz")
    val vcfFormat = new VCFFileFormat()
    val csvFormat = new CSVFileFormat()

    assert(vcfFormat.isSplitable(spark, Map.empty, gzPath))
    assert(!csvFormat.isSplitable(spark, Map.empty, gzPath))

    assert(vcfFormat.isSplitable(spark, Map.empty, bgzPath))
    assert(csvFormat.isSplitable(spark, Map.empty, bgzPath))
  }

  test("uncompressed files are splitable") {
    val path = new Path(s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf")
    val vcfFormat = new VCFFileFormat()
    assert(vcfFormat.isSplitable(spark, Map.empty, path))
  }

  test("Tolerate lower-case nan's") {
    val sess = spark
    import sess.implicits._

    val vcfRows = spark
      .read
      .format(sourceName)
      .schema(VCFRow.schema)
      .load(s"$testDataHome/vcf/test_withNanQual.vcf")
      .as[VCFRow]
      .collect()

    vcfRows.foreach { vc =>
      assert(vc.qual.get.isNaN)
    }
  }

  private def compareRows(r1: VCFRow, r2: VCFRow): Unit = {
    assert(r1.copy(qual = None) == r2.copy(qual = None))
    assert(r1.qual.isDefined == r2.qual.isDefined)
    for {
      q1 <- r1.qual
      q2 <- r2.qual
    } {
      assert(q1 ~== q2 relTol 0.2)
    }
  }

  test("Parse VEP") {
    val vcf = spark
      .read
      .format(sourceName)
      .load(s"$testDataHome/vcf/loftee.vcf")

    val csqs = vcf.selectExpr("explode(INFO_CSQ)").collect()
    assert(csqs.length == 23)
    assert(
      csqs.head.getStruct(0).toSeq == Seq(
        "T",
        Seq("missense_variant", "splice_region_variant"),
        "MODERATE",
        "CHEK2",
        "ENSG00000183765",
        "Transcript",
        "ENST00000328354",
        "protein_coding",
        Row(11, 15),
        null,
        null,
        null,
        1341,
        1259,
        420,
        Row("C", "Y"),
        Row("tGc", "tAc"),
        null,
        null,
        -1,
        null,
        "HGNC",
        "16627",
        null,
        null,
        null,
        Seq(
          "EXON_START:29091698",
          "DONOR_GERP_DIFF:0",
          "DONOR_MES_DIFF:4.2436768980804",
          "BRANCHPOINT_DISTANCE:NA",
          "DONOR_ISS:4",
          "INTRON_END:29091697",
          "DONOR_DISRUPTION_PROB:0.892289929287647",
          "DONOR_ESE:27",
          "MUTANT_DONOR_MES:5.20491527083293",
          "DONOR_ISE:7",
          "EXON_END:29091861",
          "DONOR_ESS:34",
          "INTRON_START:29091231"
        )
      ))
  }

  test("Parse SnpEff") {
    val vcf = spark
      .read
      .format(sourceName)
      .load(s"$testDataHome/vcf/snpeff.vcf")

    val anns = vcf.selectExpr("explode(INFO_ANN)").collect()
    assert(anns.length == 5)
    assert(
      anns.head.getStruct(0).toSeq == Seq(
        "T",
        Seq("splice_region_variant", "synonymous_variant"),
        "LOW",
        "GAB4",
        "ENSG00000215568",
        "transcript",
        "ENST00000400588",
        "protein_coding",
        Row(6, 10),
        "c.1287G>A",
        "p.Lys429Lys",
        Row(1395, null),
        Row(1287, 1725),
        Row(429, 574),
        null,
        null
      ))

    assert(
      anns(1).getStruct(0).toSeq == Seq(
        "T",
        Seq("splice_region_variant", "non_coding_exon_variant"),
        "LOW",
        "GAB4",
        "ENSG00000215568",
        "transcript",
        "ENST00000465611",
        "nonsense_mediated_decay",
        Row(5, 9),
        "n.*1412G>A",
        null,
        null,
        null,
        null,
        null,
        null
      ))
  }

  test("Do not break when reading index file") {
    spark
      .read
      .format(sourceName)
      .load(s"$testDataHome/tabix-test-vcf/NA12878_21_10002403.vcf.gz.tbi")
  }

  test("Do not break when reading directory with index files") {
    spark.read.format(sourceName).load(s"$testDataHome/tabix-test-vcf")
  }

  test("Do not break when reading VCFs with contig lines missing length") {
    // Read two copies of the same file to trigger a header line merge
    // May break if we parse contig header lines missing length
    spark
      .read
      .format(sourceName)
      .load(
        s"$testDataHome/vcf/missing_contig_length.vcf",
        s"$testDataHome/vcf/missing_contig_length.vcf")
  }
}

// For testing only: schema based on CEUTrio VCF header
private case class INFOFields(
    INFO_AC: Seq[Option[Int]],
    INFO_AF: Seq[Option[Double]],
    INFO_AN: Option[Int],
    INFO_BaseQRankSum: Option[Double],
    INFO_DP: Option[Int],
    INFO_DS: Option[Boolean],
    INFO_ExcessHet: Option[Double],
    INFO_FS: Option[Double],
    INFO_InbreedingCoeff: Option[Double],
    INFO_MLEAC: Seq[Option[Int]],
    INFO_MLEAF: Seq[Option[Double]],
    INFO_MQ: Option[Double],
    INFO_MQRankSum: Option[Double],
    INFO_QD: Option[Double],
    INFO_ReadPosRankSum: Option[Double],
    INFO_SOR: Option[Double])
