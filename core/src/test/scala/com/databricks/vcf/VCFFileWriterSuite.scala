package com.databricks.vcf

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
import org.apache.spark.{SparkConf, SparkException}
import org.bdgenomics.adam.rdd.ADAMContext._
import com.databricks.hls.common.WithUtils
import com.databricks.hls.sql.HLSBaseTest
import org.apache.spark.sql.types.StructType
import org.bdgenomics.adam.rdd.VCFMetadataLoader

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
        .format(writeSourceName)
        .save(tempFile)
    } else {
      repartitioned
        .write
        .format(writeSourceName)
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

  test("Empty DF and missing sample IDs") {
    val tempFile = createTempVcf.toString

    val ds = spark
      .read
      .format(readSourceName)
      .option("includeSampleIds", false)
      .load(NA12878)
      .limit(0)

    // No VCF rows and missing samples
    assertThrows[SparkException](
      ds.write
        .format(writeSourceName)
        .save(tempFile)
    )
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
        .format(writeSourceName)
        .option("validationStringency", "fakeStringency")
        .save(tempFile)
    )
  }

  test("Corrupted header lines are not written") {
    val sc = spark.sparkContext
    val sess = spark

    val tempFile = createTempVcf.toString

    val headerLine = new VCFHeaderLine("fakeHeaderKey", "fakeHeaderValue")
    val extraHeader = new VCFHeader(Set(headerLine).asJava)
    val extraHeaderStr = VCFHeaderWriter.writeHeaderAsString(extraHeader)
    sess
      .read
      .format(readSourceName)
      .load(NA12878)
      .write
      .format(writeSourceName)
      .option("vcfHeader", extraHeaderStr.substring(0, extraHeaderStr.length - 10))
      .save(tempFile)

    val vcRdd = sc.loadVcf(tempFile)
    assert(!vcRdd.headerLines.contains(headerLine))
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
        .option("vcfHeader", NA12878)
        .format("com.databricks.vcf")
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
        .format(writeSourceName)
        .option("vcfHeader", vcfHeaderOpt.get)
        .save(tempFileStr)
    } else {
      df.write
        .format(writeSourceName)
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

  test("Provided header") {
    val headerLine1 = new VCFHeaderLine("fakeHeaderKey", "fakeHeaderValue")
    val headerLine2 = new VCFHeaderLine("secondFakeHeaderKey", "secondFakeHeaderValue")
    val headerLines = Set(headerLine1, headerLine2)
    val extraHeader = new VCFHeader(headerLines.asJava, Seq("sample1", "NA12878").asJava)
    val vcfHeader = VCFHeaderWriter.writeHeaderAsString(extraHeader)
    val writtenHeader =
      writeVcfHeader(spark.read.format(readSourceName).load(NA12878), Some(vcfHeader))

    assert(headerLines.subsetOf(writtenHeader.getMetaDataInInputOrder.asScala))
    assert(writtenHeader.getGenotypeSamples.asScala == Seq("sample1", "NA12878"))
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
}

class MultiFileVCFWriterSuite extends VCFFileWriterSuite {
  override def writeSourceName: String = "com.databricks.vcf"

  test("Empty file") {
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    spark
      .sparkContext
      .emptyRDD[VCFRow]
      .toDS
      .write
      .option("vcfHeader", NA12878)
      .format(writeSourceName)
      .save(tempFile)
    val rewrittenDs = spark
      .read
      .format(readSourceName)
      .option("vcfRowSchema", true)
      .load(tempFile)
    assert(rewrittenDs.collect.isEmpty)
  }
}

class SingleFileVCFWriterSuite extends VCFFileWriterSuite {
  override def writeSourceName: String = "com.databricks.bigvcf"

  test("Check BGZF") {
    val df = spark
      .read
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

class VCFWriterUtilsSuite extends HLSBaseTest {
  val vcf = s"$testDataHome/NA12878_21_10002403.vcf"
  lazy val schema = spark.read.format("com.databricks.vcf").load(vcf).schema

  private def getHeaderNoSamples(
      options: Map[String, String],
      defaultOpt: Option[String],
      schema: StructType): VCFHeader = {
    val (headerLines, _) = VCFFileWriter.parseHeaderLinesAndSamples(
      options,
      defaultOpt,
      schema,
      spark.sparkContext.hadoopConfiguration)
    new VCFHeader(headerLines.asJava)
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
    val parsed = VCFFileWriter.parseHeaderFromString(contents)
    assert(getAllLines(header).nonEmpty)
    assert(getAllLines(header) == getAllLines(parsed))
  }
}
