package com.databricks.vcf

import java.io.BufferedInputStream
import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import com.google.common.io.ByteStreams
import htsjdk.samtools.ValidationStringency
import htsjdk.samtools.util.{BlockCompressedInputStream, BlockCompressedStreamConstants}
import htsjdk.variant.variantcontext.writer.VCFHeaderWriter
import htsjdk.variant.vcf.{VCFHeader, VCFHeaderLine}
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkException}
import org.bdgenomics.adam.rdd.ADAMContext._
import com.databricks.hls.common.WithUtils
import com.databricks.hls.sql.HLSBaseTest

abstract class VCFFileWriterSuite extends HLSBaseTest with VCFConverterBaseTest {

  lazy val NA12878 = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val TGP = s"$testDataHome/1000genomes-phase3-1row.vcf"
  val readSourceName = "com.databricks.vcf"

  override def sparkConf: SparkConf = {
    // Verify that tests correctly set BGZF codecs
    super.sparkConf.set("spark.hadoop.io.compression.codecs", "")
  }

  protected def writeSourceName: String

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

    val ds = spark.read
      .format(readSourceName)
      .option("includeSampleIds", readSampleIds)
      .option(schemaOption._1, schemaOption._2)
      .load(vcf)

    val repartitioned = partitions.map(p => ds.repartition(p)).getOrElse(ds)

    if (readSampleIds) {
      val originalHeader = scala.io.Source
        .fromFile(vcf)
        .getLines()
        .takeWhile(_.startsWith("#"))
        .mkString("\n")

      repartitioned.write
        .option("header", originalHeader)
        .format(writeSourceName)
        .save(tempFile)
    } else {
      repartitioned.write
        .format(writeSourceName)
        .save(tempFile)
    }

    val rewrittenDs = spark.read
      .format(readSourceName)
      .option("includeSampleIds", rereadSampleIds)
      .option(schemaOption._1, schemaOption._2)
      .load(tempFile)
    (ds, rewrittenDs)
  }

  private val schemaOptions = Seq(("vcfRowSchema", "true"), ("flattenInfoFields", "true"), ("", ""))

  gridTest("Read single sample VCF with VCF parser")(schemaOptions) { schema =>
    val (ds, rewrittenDs) = writeAndRereadWithDBParser(NA12878, schemaOption = schema)
    ds.show()
    rewrittenDs.show()
    ds.collect.zip(rewrittenDs.collect).foreach {
      case (vc1, vc2) =>
        assert(vc1.equals(vc2), s"VC1\n$vc1\nVC2\n$vc2")
    }
  }

  gridTest("Read multi-sample VCF with VCF parser")(schemaOptions) { schema =>
    val (ds, rewrittenDs) = writeAndRereadWithDBParser(TGP, schemaOption = schema)
    ds.collect.zip(rewrittenDs.collect).foreach {
      case (vc1, vc2) =>
        assert(vc1.equals(vc2), s"VC1 $vc1 VC2 $vc2")
    }
  }

  test("Use VCF parser without sample IDs") {
    val sess = spark
    import sess.implicits._
    val (ds, rewrittenDs) = writeAndRereadWithDBParser(
      NA12878,
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

  test("Invalid validation stringency") {
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    val ds = spark.read
      .format(readSourceName)
      .option("includeSampleIds", true)
      .option("vcfRowSchema", true)
      .load(NA12878)
      .as[VCFRow]
    assertThrows[IllegalArgumentException](
      ds.write
        .format(writeSourceName)
        .option("validationStringency", "fakeStringency")
        .save(tempFile)
    )
  }

  test("Provided header") {
    val sc = spark.sparkContext
    val sess = spark

    val tempFile = createTempVcf.toString

    val headerLine1 = new VCFHeaderLine("fakeHeaderKey", "fakeHeaderValue")
    val headerLine2 = new VCFHeaderLine("secondFakeHeaderKey", "secondFakeHeaderValue")
    val headerLines = Set(headerLine1, headerLine2)
    val extraHeader = new VCFHeader(headerLines.asJava, Seq("sample1", "NA12878").asJava)
    sess.read
      .format(readSourceName)
      .load(NA12878)
      .write
      .format(writeSourceName)
      .option("header", VCFHeaderWriter.writeHeaderAsString(extraHeader))
      .save(tempFile)

    val vcRdd = sc.loadVcf(tempFile)
    assert(headerLines.subsetOf(vcRdd.headerLines.toSet)) // ADAM mixes in supported header lines
    assert(vcRdd.samples.map(_.getId) == Seq("sample1", "NA12878"))
  }

  test("Corrupted header lines are not written") {
    val sc = spark.sparkContext
    val sess = spark

    val tempFile = createTempVcf.toString

    val headerLine = new VCFHeaderLine("fakeHeaderKey", "fakeHeaderValue")
    val extraHeader = new VCFHeader(Set(headerLine).asJava)
    val extraHeaderStr = VCFHeaderWriter.writeHeaderAsString(extraHeader)
    sess.read
      .format(readSourceName)
      .load(NA12878)
      .write
      .format(writeSourceName)
      .option("header", extraHeaderStr.substring(0, extraHeaderStr.length - 10))
      .save(tempFile)

    val vcRdd = sc.loadVcf(tempFile)
    assert(!vcRdd.headerLines.contains(headerLine))
  }

  gridTest("Strict validation stringency")(schemaOptions) { schema =>
    val tempFile = createTempVcf.toString

    val ds = spark.read
      .format(readSourceName)
      .option("includeSampleIds", true)
      .option(schema._1, schema._2)
      .load(NA12878)
    // Contains INFO and FORMAT keys (eg. INFO AC) not included in default VCFHeader
    assertThrows[SparkException](
      ds.write.format(writeSourceName).option("validationStringency", "strict").save(tempFile)
    )
  }

  Seq(("bgzf", ".bgz"), ("gzip", ".gz")).foreach {
    case (codecName, extension) =>
      test(s"Output $codecName compressed file") {
        val tempFilePath = createTempVcf

        val ds = spark.read.format(readSourceName).option("vcfRowSchema", true).load(TGP)
        val outpath = tempFilePath.toString + extension
        ds.write
          .format(writeSourceName)
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
        .format("com.databricks.vcf")
        .save(Files.createTempDirectory("vcf").resolve("vcf").toString)
    }

    parseRow(ValidationStringency.SILENT)
    parseRow(ValidationStringency.LENIENT)
    intercept[SparkException] {
      parseRow(ValidationStringency.STRICT)
    }
  }
}

class MultiFileVCFWriterSuite extends VCFFileWriterSuite {
  override def writeSourceName: String = "com.databricks.vcf"

  test("Empty file") {
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    spark.sparkContext
      .emptyRDD[VCFRow]
      .toDS
      .write
      .format(writeSourceName)
      .save(tempFile)
    val rewrittenDs = spark.read
      .format(readSourceName)
      .option("vcfRowSchema", true)
      .load(tempFile)
    assert(rewrittenDs.collect.isEmpty)
  }
}

class SingleFileVCFWriterSuite extends VCFFileWriterSuite {
  override def writeSourceName: String = "com.databricks.bigvcf"

  test("Check BGZF") {
    val df = spark.read
      .format(readSourceName)
      .load(NA12878)
      .repartition(100) // Force multiple partitions

    val outPath = createTempVcf.toString + ".bgz"
    df.write.format(writeSourceName).save(outPath)

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
}
