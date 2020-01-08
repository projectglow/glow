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

import java.io.{BufferedInputStream, File}
import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors

import scala.collection.JavaConverters._

import com.google.common.io.ByteStreams
import htsjdk.samtools.ValidationStringency
import htsjdk.samtools.util.{BlockCompressedInputStream, BlockCompressedStreamConstants}
import htsjdk.variant.variantcontext.writer.VCFHeaderWriter
import htsjdk.variant.vcf.{VCFCompoundHeaderLine, VCFHeader, VCFHeaderLine}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.{SparkConf, SparkException}

import io.projectglow.common.{VCFRow, VariantSchemas, WithUtils}
import io.projectglow.sql.GlowBaseTest

abstract class VCFFileWriterSuite(val sourceName: String)
    extends GlowBaseTest
    with VCFConverterBaseTest {

  lazy val NA12878 = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val TGP = s"$testDataHome/1000genomes-phase3-1row.vcf"
  val readSourceName = "vcf"

  override def sparkConf: SparkConf = {
    // Verify that tests correctly set BGZF codecs
    super.sparkConf.set("spark.hadoop.io.compression.codecs", "")
  }

  protected def createTempVcf: Path = {
    val tempDir = Files.createTempDirectory("test-vcf-dir")
    val path = tempDir.resolve("test.vcf")
    logger.info(s"Writing VCF to path ${path.toAbsolutePath.toString}")
    path
  }

  private def writeAndRereadWithDBParser(
      vcf: String,
      readSampleIds: Boolean = true,
      rereadSampleIds: Boolean = true,
      schemaOption: (String, String),
      partitions: Option[Int] = None): (DataFrame, DataFrame) = {

    val tempFile = createTempVcf.toString

    val ds = spark
      .read
      .format(readSourceName)
      .option("includeSampleIds", readSampleIds)
      .option(schemaOption._1, schemaOption._2)
      .load(vcf)

    val repartitioned = partitions.map(p => ds.repartition(p)).getOrElse(ds)

    if (readSampleIds) {
      val originalHeader = scala
        .io
        .Source
        .fromFile(vcf)
        .getLines()
        .takeWhile(_.startsWith("#"))
        .mkString("\n")

      repartitioned
        .write
        .option("vcfHeader", originalHeader)
        .format(sourceName)
        .save(tempFile)
    } else {
      repartitioned
        .write
        .format(sourceName)
        .save(tempFile)
    }

    val rewrittenDs = spark
      .read
      .format(readSourceName)
      .option("includeSampleIds", rereadSampleIds)
      .option(schemaOption._1, schemaOption._2)
      .load(tempFile)
    (ds, rewrittenDs)
  }

  private val schemaOptions = Seq(("vcfRowSchema", "true"), ("flattenInfoFields", "true"), ("", ""))

  gridTest("Read single sample VCF with VCF parser")(schemaOptions) { schema =>
    val (ds, rewrittenDs) = writeAndRereadWithDBParser(NA12878, schemaOption = schema)
    ds.collect()
    rewrittenDs.collect()
    ds.collect.zip(rewrittenDs.collect).foreach {
      case (vc1, vc2) =>
        assert(vc1.equals(vc2), s"VC1\n$vc1\nVC2\n$vc2")
    }
  }

  gridTest("Read multi-sample VCF with VCF parser")(schemaOptions) { schema =>
    val (ds, rewrittenDs) =
      writeAndRereadWithDBParser(s"$testDataHome/vcf/VCFv4.3.vcf", schemaOption = schema)
    ds.collect.zip(rewrittenDs.collect).foreach {
      case (vc1, vc2) =>
        assert(vc1.equals(vc2), s"VC1 $vc1 VC2 $vc2")
    }
  }

  gridTest("Read VEP VCF with VCF parser")(schemaOptions) { schema =>
    val (ds, rewrittenDs) =
      writeAndRereadWithDBParser(s"$testDataHome/vcf/loftee.vcf", schemaOption = schema)
    ds.collect.zip(rewrittenDs.collect).foreach {
      case (vc1, vc2) =>
        assert(vc1.equals(vc2), s"VC1 $vc1 VC2 $vc2")
    }
  }

  gridTest("Read SnpEff VCF with VCF parser")(schemaOptions) { schema =>
    val (ds, rewrittenDs) =
      writeAndRereadWithDBParser(s"$testDataHome/vcf/snpeff.vcf", schemaOption = schema)
    ds.collect.zip(rewrittenDs.collect).foreach {
      case (vc1, vc2) =>
        assert(vc1.equals(vc2), s"VC1 $vc1 VC2 $vc2")
    }
  }

  test("Use VCF parser without sample IDs") {
    val sess = spark
    import sess.implicits._
    val (ds, rewrittenDs) = writeAndRereadWithDBParser(
      TGP,
      readSampleIds = false,
      schemaOption = ("vcfRowSchema", "true")
    )
    ds.as[VCFRow].collect.zip(rewrittenDs.as[VCFRow].collect).foreach {
      case (vc1, vc2) =>
        var missingSampleIdx = 0
        val gtsWithSampleIds = vc1.genotypes.map { gt =>
          missingSampleIdx += 1
          gt.copy(sampleId = Some(s"sample_$missingSampleIdx"))
        }
        val vc1WithSampleIds = vc1.copy(genotypes = gtsWithSampleIds)
        assert(vc1WithSampleIds.equals(vc2), s"VC1 $vc1WithSampleIds VC2 $vc2")
    }
  }

  test("Use VCF parser without sample IDs (many partitions)") {
    val sess = spark
    import sess.implicits._
    val (ds, rewrittenDs) = writeAndRereadWithDBParser(
      NA12878,
      readSampleIds = false,
      schemaOption = ("vcfRowSchema", "true"),
      partitions = Some(100)
    )
    val orderedDs1 = ds.orderBy("contigName", "start")
    val orderedDs2 = rewrittenDs.orderBy("contigName", "start")
    orderedDs1.as[VCFRow].collect.zip(orderedDs2.as[VCFRow].collect).foreach {
      case (vc1, vc2) =>
        var missingSampleIdx = 0
        val gtsWithSampleIds = vc1.genotypes.map { gt =>
          missingSampleIdx += 1
          gt.copy(sampleId = Some(s"sample_$missingSampleIdx"))
        }
        val vc1WithSampleIds = vc1.copy(genotypes = gtsWithSampleIds)
        assert(vc1WithSampleIds.equals(vc2), s"VC1 $vc1WithSampleIds VC2 $vc2")
    }
  }

  test("Use VCF parser with sample IDs (many partitions)") {
    val sess = spark
    import sess.implicits._
    val (ds, rewrittenDs) = writeAndRereadWithDBParser(
      NA12878,
      schemaOption = ("vcfRowSchema", "true"),
      partitions = Some(100)
    )
    val orderedDs1 = ds.orderBy("contigName", "start")
    val orderedDs2 = rewrittenDs.orderBy("contigName", "start")
    orderedDs1.as[VCFRow].collect.zip(orderedDs2.as[VCFRow].collect).foreach {
      case (vc1, vc2) => assert(vc1.equals(vc2), s"VC1 $vc1 VC2 $vc2")
    }
  }

  test("Strict validation stringency") {
    val tempFile = createTempVcf.toString

    val ds = spark
      .read
      .format(readSourceName)
      .option("includeSampleIds", true)
      .option("flattenInfoFields", false)
      .load(NA12878)
    // Contains INFO and FORMAT keys (eg. INFO AC) that can't be inferred from attributes map
    assertThrows[SparkException](
      ds.write.format(sourceName).option("validationStringency", "strict").save(tempFile)
    )
  }

  Seq(("bgzf", ".bgz"), ("gzip", ".gz")).foreach {
    case (codecName, extension) =>
      test(s"Output $codecName compressed file") {
        val tempFilePath = createTempVcf

        val ds = spark.read.format(readSourceName).option("vcfRowSchema", true).load(TGP)
        val outpath = tempFilePath.toString + extension
        ds.write
          .format(sourceName)
          .option("compression", codecName)
          .save(outpath)

        val outFile = Paths.get(outpath)
        val filesWritten = if (outFile.toFile.isDirectory) {
          Files.list(Paths.get(outpath)).collect(Collectors.toList[Path]).asScala.map(_.toString)
        } else {
          Seq(outpath)
        }
        assert(filesWritten.exists(s => s.endsWith(extension)))
      }
  }

  test("variant context validation settings obey stringency") {
    def parseRow(stringency: ValidationStringency): Unit = {
      val data =
        VCFRow(null, 0, 1, Seq.empty, null, Seq.empty, None, Seq.empty, Map.empty, Seq.empty)
      spark
        .createDataFrame(Seq(data))
        .drop("contigName")
        .write
        .mode("overwrite")
        .option("validationStringency", stringency.toString)
        .option("vcfHeader", NA12878)
        .format(sourceName)
        .save(Files.createTempDirectory("vcf").resolve("vcf").toString)
    }

    parseRow(ValidationStringency.SILENT)
    parseRow(ValidationStringency.LENIENT)
    intercept[SparkException] {
      parseRow(ValidationStringency.STRICT)
    }
  }

  def writeVcfHeader(df: DataFrame, vcfHeaderOpt: Option[String]): VCFHeader = {
    val tempFileStr = createTempVcf.toString

    if (vcfHeaderOpt.isDefined) {
      df.write
        .format(sourceName)
        .option("vcfHeader", vcfHeaderOpt.get)
        .save(tempFileStr)
    } else {
      df.write
        .format(sourceName)
        .save(tempFileStr)
    }

    val tempFile = new File(tempFileStr)
    val fileToRead = if (tempFile.isDirectory) {
      tempFile.listFiles().filter(_.getName.endsWith(".vcf")).head.getAbsolutePath
    } else {
      tempFileStr
    }
    VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, fileToRead)
  }

  private def getSchemaLines(header: VCFHeader): Set[VCFCompoundHeaderLine] = {
    header.getInfoHeaderLines.asScala.toSet ++ header.getFormatHeaderLines.asScala.toSet
  }

  test("Provided header is sorted") {
    val headerLine1 = new VCFHeaderLine("fakeHeaderKey", "fakeHeaderValue")
    val headerLine2 = new VCFHeaderLine("secondFakeHeaderKey", "secondFakeHeaderValue")
    val headerLines = Set(headerLine1, headerLine2)
    val extraHeader = new VCFHeader(headerLines.asJava, Seq("sample1", "NA12878").asJava)
    val vcfHeader = VCFHeaderWriter.writeHeaderAsString(extraHeader)
    val writtenHeader =
      writeVcfHeader(spark.read.format(readSourceName).load(NA12878), Some(vcfHeader))

    assert(headerLines.subsetOf(writtenHeader.getMetaDataInInputOrder.asScala))
    assert(writtenHeader.getGenotypeSamples.asScala == Seq("NA12878", "sample1"))
  }

  test("Path header") {
    val writtenHeader = writeVcfHeader(spark.read.format(readSourceName).load(NA12878), Some(TGP))
    val tgpHeader = VCFMetadataLoader.readVcfHeader(spark.sparkContext.hadoopConfiguration, TGP)
    assert(tgpHeader.getMetaDataInInputOrder == writtenHeader.getMetaDataInInputOrder)
    assert(tgpHeader.getGenotypeSamples == writtenHeader.getGenotypeSamples)
  }

  test("Infer header") {
    val df = spark.read.format(readSourceName).load(NA12878)
    val writtenHeader = writeVcfHeader(df, Some("infer"))
    val oldHeader = VCFMetadataLoader.readVcfHeader(spark.sparkContext.hadoopConfiguration, NA12878)
    assert(
      getSchemaLines(writtenHeader) == VCFSchemaInferrer.headerLinesFromSchema(df.schema).toSet)
    assert(getSchemaLines(writtenHeader) == getSchemaLines(oldHeader)) // Includes descriptions
  }

  test("Empty file with inferred header") {
    val tempFile = createTempVcf.toString

    val ds = spark
      .read
      .format(readSourceName)
      .load(NA12878)
      .limit(0)

    // Cannot infer sample IDs without rows
    assertThrows[SparkException](
      ds.write
        .format(sourceName)
        .save(tempFile)
    )
  }

  test("Empty file with determined header") {
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    spark
      .sparkContext
      .emptyRDD[VCFRow]
      .toDS
      .repartition(1)
      .write
      .option("vcfHeader", NA12878)
      .format(sourceName)
      .save(tempFile)

    val rewrittenDs = spark
      .read
      .format(readSourceName)
      .option("vcfRowSchema", true)
      .load(tempFile)

    assert(rewrittenDs.collect.isEmpty)
  }

  test("No genotypes column") {
    val tempFile = createTempVcf.toString

    val df = spark
      .read
      .format(readSourceName)
      .load(TGP)
      .drop("genotypes")
    df.write.format(sourceName).save(tempFile)

    val rereadDf = spark.read.format(readSourceName).load(tempFile)
    assert(!rereadDf.schema.contains("genotypes"))
  }

  test("No sample IDs column") {
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    val df = spark
      .read
      .format(readSourceName)
      .option("includeSampleIds", "false")
      .load(TGP)
      .withColumn("subsetGenotypes", expr("slice(genotypes, 1, 3)"))
      .drop("genotypes")
      .withColumnRenamed("subsetGenotypes", "genotypes")
    df.write.format(sourceName).save(tempFile)

    val rereadDf = spark.read.format(readSourceName).load(tempFile)
    val sampleIds = rereadDf.select("genotypes.sampleId").distinct().as[Seq[String]].collect
    assert(sampleIds.length == 1)
    assert(sampleIds.head == Seq("sample_1", "sample_2", "sample_3"))
  }
}

class MultiFileVCFWriterSuite extends VCFFileWriterSuite("vcf") {

  test("Corrupted header lines are not written") {
    val tempFile = createTempVcf.toString

    val extraHeaderStr = VCFHeaderWriter.writeHeaderAsString(new VCFHeader())
    val e = intercept[SparkException] {
      spark
        .read
        .format(readSourceName)
        .load(NA12878)
        .write
        .format(sourceName)
        .option("vcfHeader", extraHeaderStr.substring(0, extraHeaderStr.length - 10))
        .save(tempFile)
    }
    assert(e.getCause.getMessage.contains("Unable to parse VCF header"))
  }

  test("Invalid validation stringency") {
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    val ds = spark
      .read
      .format(readSourceName)
      .option("includeSampleIds", true)
      .option("vcfRowSchema", true)
      .load(NA12878)
      .as[VCFRow]
    assertThrows[SparkException](
      ds.write
        .format(sourceName)
        .option("validationStringency", "fakeStringency")
        .save(tempFile)
    )
  }

  test("Some empty partitions and infer sample IDs") {
    val tempFile = createTempVcf.toString

    val ds = spark
      .read
      .format(readSourceName)
      .load(NA12878)
      .limit(2)
      .repartition(5)

    assertThrows[SparkException](
      ds.write
        .format(sourceName)
        .save(tempFile)
    )
  }

  def testInferredSampleIds(row1HasSamples: Boolean, row2HasSamples: Boolean): Unit = {
    val tempFile = createTempVcf.toString

    // Samples: HG00096 HG00097	HG00099
    val ds1 = spark
      .read
      .format(readSourceName)
      .option("includeSampleIds", row1HasSamples)
      .option("vcfRowSchema", true)
      .load(TGP)
      .withColumn("subsetGenotypes", expr("slice(genotypes, 1, 3)"))
      .drop("genotypes")
      .withColumnRenamed("subsetGenotypes", "genotypes")

    // Samples: HG00099 HG00100	HG00101	HG00102
    val ds2 = spark
      .read
      .format(readSourceName)
      .option("includeSampleIds", row2HasSamples)
      .option("vcfRowSchema", true)
      .load(TGP)
      .withColumn("subsetGenotypes", expr("slice(genotypes, 3, 4)"))
      .drop("genotypes")
      .withColumnRenamed("subsetGenotypes", "genotypes")

    val ds = ds1.union(ds2).repartition(1)

    val e = intercept[SparkException] {
      ds.write
        .option("vcfHeader", "infer")
        .format(sourceName)
        .save(tempFile)
    }
    assert(e.getCause.getCause.getCause.isInstanceOf[IllegalArgumentException])
    assert(
      e.getCause
        .getMessage
        .contains("Cannot infer sample ids because they are not the same in every row"))
  }

  test("Fails if inferred present sample IDs but row missing sample IDs") {
    testInferredSampleIds(true, false)
  }

  test("Fails if inferred present sample IDs but row has different sample IDs") {
    testInferredSampleIds(true, true)
  }

  test("Fails if injected missing sample IDs don't match number of samples") {
    testInferredSampleIds(false, false)
  }

  test("Fails if injected missing sample IDs but has sample IDs") {
    testInferredSampleIds(false, true)
  }
}

class SingleFileVCFWriterSuite extends VCFFileWriterSuite("bigvcf") {

  test("Corrupted header lines are not written") {
    val tempFile = createTempVcf.toString

    val extraHeaderStr = VCFHeaderWriter.writeHeaderAsString(new VCFHeader())
    val e = intercept[IllegalArgumentException] {
      spark
        .read
        .format(readSourceName)
        .load(NA12878)
        .write
        .format(sourceName)
        .option("vcfHeader", extraHeaderStr.substring(0, extraHeaderStr.length - 10))
        .save(tempFile)
    }
    assert(e.getMessage.contains("Unable to parse VCF header"))
  }

  test("Invalid validation stringency") {
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    val ds = spark
      .read
      .format(readSourceName)
      .option("includeSampleIds", true)
      .option("vcfRowSchema", true)
      .load(NA12878)
      .as[VCFRow]
    assertThrows[IllegalArgumentException](
      ds.write
        .format(sourceName)
        .option("validationStringency", "fakeStringency")
        .save(tempFile)
    )
  }

  test("Check BGZF") {
    val df = spark
      .read
      .format(readSourceName)
      .load(NA12878)
      .repartition(100) // Force multiple partitions

    val outPath = createTempVcf.toString + ".bgz"
    df.write.format(sourceName).save(outPath)

    val path = new org.apache.hadoop.fs.Path(outPath)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    WithUtils.withCloseable(new BufferedInputStream(fs.open(path))) { is =>
      // Contains block gzip header
      assert(BlockCompressedInputStream.isValidFile(is))
      val bytes = ByteStreams.toByteArray(is)

      // Empty gzip block only occurs once
      assert(
        bytes.indexOfSlice(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK) ==
        bytes.lastIndexOfSlice(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK)
      )

      // Empty gzip block is at end of file
      assert(bytes.endsWith(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK))
    }
  }

  test("Some empty partitions and by default infer sample IDs") {
    val tempFile = createTempVcf.toString

    val ds = spark
      .read
      .format(readSourceName)
      .load(NA12878)
      .limit(2)
      .repartition(5)

    ds.write
      .format(sourceName)
      .save(tempFile)

    val rereadDs = spark.read.format("vcf").load(tempFile)
    assert(rereadDs.sort("start").collect sameElements ds.sort("start").collect)
  }

  test("Some empty partitions and explicitly infer sample IDs") {
    val tempFile = createTempVcf.toString

    val ds = spark
      .read
      .format(readSourceName)
      .load(NA12878)
      .limit(2)
      .repartition(5)

    ds.write
      .option("vcfHeader", "infer")
      .format(sourceName)
      .save(tempFile)

    val rereadDs = spark.read.format("vcf").load(tempFile)
    assert(rereadDs.sort("start").collect sameElements ds.sort("start").collect)
  }

  test("Bigvcf header check for empty file with determined header") {
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    spark
      .sparkContext
      .emptyRDD[VCFRow]
      .toDS
      .repartition(1)
      .write
      .option("vcfHeader", NA12878)
      .format(sourceName)
      .save(tempFile)

    val truthHeader = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, NA12878)
    val writtenHeader = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, tempFile)

    assert(truthHeader.getMetaDataInInputOrder.equals(writtenHeader.getMetaDataInInputOrder))
    assert(truthHeader.getGenotypeSamples == writtenHeader.getGenotypeSamples)
  }

  test("Bigvcf 0 partitions exception check") {
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    assertThrows[SparkException](
      spark
        .sparkContext
        .emptyRDD[VCFRow]
        .toDS
        .write
        .option("vcfHeader", NA12878)
        .format(sourceName)
        .save(tempFile)
    )
  }

  def sliceInferredSampleIds(
      start: Int,
      numPresentSampleIds: Int,
      numMissingSampleIds: Int): DataFrame = {
    val presentSampleIds = spark
      .read
      .format(readSourceName)
      .option("vcfRowSchema", "true")
      .load(TGP)
      .withColumn("genotypesWithSampleIds", expr(s"slice(genotypes, $start, $numPresentSampleIds)"))
      .drop("genotypes")

    val missingSampleIds = spark
      .read
      .format(readSourceName)
      .option("includeSampleIds", "false")
      .option("vcfRowSchema", "true")
      .load(TGP)
      .withColumn(
        "genotypesWithoutSampleIds",
        expr(s"slice(genotypes, $start, $numMissingSampleIds)"))
      .drop("genotypes", "attributes")

    presentSampleIds
      .join(missingSampleIds, VariantSchemas.vcfBaseSchema.map(_.name))
      .withColumn("genotypes", expr("concat(genotypesWithSampleIds, genotypesWithoutSampleIds)"))
      .drop("genotypesWithSampleIds", "genotypesWithoutSampleIds")
  }

  def checkWithInferredSampleIds(df: DataFrame, expectedSampleIds: Seq[String]): Unit = {
    val tempFile = createTempVcf.toString

    // Should be written with all samples with no-calls if sample is missing
    df.write
      .option("vcfHeader", "infer")
      .format(sourceName)
      .save(tempFile)

    val rereadDf =
      spark
        .read
        .format(readSourceName)
        .option("vcfRowSchema", "true")
        .load(tempFile)

    val sess = spark
    import sess.implicits._

    // Make sure there is only one set of sample IDs and they match the expected ones
    val sampleIdRows = rereadDf.select("genotypes.sampleId").distinct().as[Seq[String]].collect
    assert(sampleIdRows.length == 1)
    assert(sampleIdRows.head == expectedSampleIds)

    // Compare the called genotypes
    val calledRereadDf = rereadDf
      .withColumn("calledGenotypes", expr("filter(genotypes, gt -> gt.calls[0] != -1)"))
      .drop("genotypes")
      .withColumnRenamed("calledGenotypes", "genotypes")

    df.as[VCFRow].collect.zip(calledRereadDf.as[VCFRow].collect).foreach {
      case (vc1, vc2) =>
        var missingSampleIdx = 0
        val gtsWithSampleIds = vc1.genotypes.map { gt =>
          if (gt.sampleId.isEmpty) {
            missingSampleIdx += 1
            gt.copy(sampleId = Some(s"sample_$missingSampleIdx"))
          } else {
            gt
          }
        }
        val vc1WithSampleIds = vc1.copy(genotypes = gtsWithSampleIds)
        assert(vc1WithSampleIds.equals(vc2), s"VC1 $vc1WithSampleIds VC2 $vc2")
    }
  }

  test("Unions inferred sample IDs") {
    // Samples: HG00096	HG00097	HG00099	HG00100	HG00101
    val ds1 = sliceInferredSampleIds(1, 5, 0)
    // Samples: HG00099	HG00100	HG00101	HG00102
    val ds2 = sliceInferredSampleIds(3, 4, 0)

    checkWithInferredSampleIds(
      ds1.union(ds2),
      Seq("HG00096", "HG00097", "HG00099", "HG00100", "HG00101", "HG00102"))
  }

  test("Matching number of missing sample IDs") {
    // 3 missing samples
    val ds1 = sliceInferredSampleIds(1, 0, 3)
    val ds2 = sliceInferredSampleIds(2, 0, 3)
    checkWithInferredSampleIds(ds1.union(ds2), Seq("sample_1", "sample_2", "sample_3"))
  }

  test("Mixed inferred and missing sample IDs") {
    // 3 missing samples
    val ds1 = sliceInferredSampleIds(1, 3, 2)
    val ds2 = sliceInferredSampleIds(2, 3, 2)

    val tempFile = createTempVcf.toString
    val e = intercept[IllegalArgumentException] {
      ds1.union(ds2).write.option("vcfHeader", "infer").format(sourceName).save(tempFile)
    }
    assert(e.getMessage.contains("Cannot mix missing and non-missing sample IDs"))
  }

  test("Non-matching number of missing sample IDs") {
    // 2 missing samples
    val ds1 = sliceInferredSampleIds(1, 0, 2)
    // 4 missing samples
    val ds2 = sliceInferredSampleIds(2, 0, 1)
    val e = intercept[IllegalArgumentException] {
      ds1.union(ds2).write.format(sourceName).save(createTempVcf.toString)
    }
    assert(e.getMessage.contains("Rows contain varying number of missing samples"))
  }
}
