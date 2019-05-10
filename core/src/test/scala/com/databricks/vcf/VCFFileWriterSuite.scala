package com.databricks.vcf

import java.nio.file.{Files, Path}
import java.util.stream.Collectors

import scala.collection.JavaConverters._

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.writer.VCFHeaderWriter
import htsjdk.variant.vcf.{VCFHeader, VCFHeaderLine}
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Dataset}
import org.bdgenomics.adam.converters.DefaultHeaderLines
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{Genotype => BdgGenotype, Variant}
import org.bdgenomics.adam.sql.{
  VariantContext,
  Genotype => GenotypeProduct,
  Variant => VariantProduct
}
import org.bdgenomics.formats.avro.GenotypeAllele

class VCFFileWriterSuite extends VCFConverterBaseTest {

  lazy val NA12878 = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val TGP = s"$testDataHome/1000genomes-phase3-1row.vcf"
  val sourceName = "com.databricks.vcf"

  private def createTempVcf: Path = {
    val tempDir = Files.createTempDirectory("test-vcf-dir")
    tempDir.resolve("test.vcf")
  }

  // Removes artifacts from multi -> biallelic variant context splitting by ADAM
  private def removeMultiAllelicInfo(vc: VariantContext): VariantContext = {
    val variantBuilder = Variant.newBuilder(vc.variant.toAvro)
    variantBuilder.setSplitFromMultiAllelic(null)
    val variantWithoutMultiAllelic = VariantProduct.fromAvro(variantBuilder.build)

    val genotypeSeq = vc.genotypes.map { gt =>
      val genotypeBuilder = BdgGenotype.newBuilder(gt.toAvro)
      val allelesWithOtherAltRemoved = genotypeBuilder.getAlleles.asScala.map { a =>
        if (a.equals(GenotypeAllele.OTHER_ALT)) {
          GenotypeAllele.NO_CALL
        } else {
          a
        }
      }
      genotypeBuilder.setAlleles(allelesWithOtherAltRemoved.asJava)
      genotypeBuilder.setSplitFromMultiAllelic(null)

      val variantBuilder = Variant.newBuilder(genotypeBuilder.getVariant)
      variantBuilder.setSplitFromMultiAllelic(null)
      genotypeBuilder.setVariant(variantBuilder.build)

      GenotypeProduct.fromAvro(genotypeBuilder.build)
    }

    vc.copy(variant = variantWithoutMultiAllelic, genotypes = genotypeSeq)
  }

  private def writeAndRereadWithAdam(
      vcf: String): (Dataset[VariantContext], Dataset[VariantContext]) = {

    val sc = spark.sparkContext

    val tempFile = createTempVcf.toString
    val ds = sc.loadVcf(vcf).dataset
    ds.write.format(sourceName).save(tempFile)

    val vcRdd = sc.loadVcf(tempFile)
    assert(DefaultHeaderLines.allHeaderLines.toSet.subsetOf(vcRdd.headerLines.toSet))
    val rewrittenDs = vcRdd.dataset
    (ds, rewrittenDs)
  }

  private def writeAndRereadWithDBParser(
      vcf: String,
      readSampleIds: Boolean = true,
      rereadSampleIds: Boolean = true,
      schemaOption: (String, String)): (DataFrame, DataFrame) = {

    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString
    val originalHeaderLines = scala.io.Source
      .fromFile(vcf)
      .getLines()
      .takeWhile(_.startsWith("#"))
      .mkString("\n")

    val ds = spark.read
      .format(sourceName)
      .option("includeSampleIds", readSampleIds)
      .option(schemaOption._1, schemaOption._2)
      .load(vcf)
    ds.write.option("overrideHeaderLines", originalHeaderLines).format(sourceName).save(tempFile)

    val rewrittenDs = spark.read
      .format(sourceName)
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

  test("Use VCF parser without samples IDs") {
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

  test("Invalid validation stringency") {
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    val ds = spark.read
      .format(sourceName)
      .option("includeSampleIds", true)
      .option("vcfRowSchema", true)
      .load(NA12878)
      .as[VCFRow]
    assertThrows[IllegalArgumentException](
      ds.write.format(sourceName).option("validationStringency", "fakeStringency").save(tempFile)
    )
  }

  test("Empty file") {
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    spark.sparkContext
      .emptyRDD[VCFRow]
      .toDS
      .write
      .format(sourceName)
      .save(tempFile)
    val rewrittenDs = spark.read
      .format(sourceName)
      .option("vcfRowSchema", true)
      .load(tempFile)
    assert(rewrittenDs.collect.isEmpty)
  }

  test("Extra header lines") {
    val sc = spark.sparkContext
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    val headerLine1 = new VCFHeaderLine("fakeHeaderKey", "fakeHeaderValue")
    val headerLine2 = new VCFHeaderLine("secondFakeHeaderKey", "secondFakeHeaderValue")
    val extraHeaderLines = Set(headerLine1, headerLine2)
    val extraHeader = new VCFHeader(extraHeaderLines.asJava)
    sc.emptyRDD[VCFRow]
      .toDS
      .write
      .format(sourceName)
      .option("extraHeaderLines", VCFHeaderWriter.writeHeaderAsString(extraHeader))
      .save(tempFile)

    val vcRdd = sc.loadVcf(tempFile)
    assert(extraHeaderLines.subsetOf(vcRdd.headerLines.toSet))
  }

  test("Corrupted extra header lines are not written") {
    val sc = spark.sparkContext
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    val headerLine = new VCFHeaderLine("fakeHeaderKey", "fakeHeaderValue")
    val extraHeader = new VCFHeader(Set(headerLine).asJava)
    val extraHeaderStr = VCFHeaderWriter.writeHeaderAsString(extraHeader)
    sc.emptyRDD[VCFRow]
      .toDS
      .write
      .format(sourceName)
      .option("extraHeaderLines", extraHeaderStr.substring(0, extraHeaderStr.length - 10))
      .save(tempFile)

    val vcRdd = sc.loadVcf(tempFile)
    assert(!vcRdd.headerLines.contains(headerLine))
  }

  gridTest("Strict validation stringency")(schemaOptions) { schema =>
    val sess = spark
    import sess.implicits._

    val tempFile = createTempVcf.toString

    val ds = spark.read
      .format(sourceName)
      .option("includeSampleIds", true)
      .option(schema._1, schema._2)
      .load(NA12878)
    // Contains INFO and FORMAT keys (eg. INFO AC) not included in default VCFHeader
    assertThrows[SparkException](
      ds.write.format(sourceName).option("validationStringency", "strict").save(tempFile)
    )
  }

  Seq(("bgzf", ".bgz"), ("gzip", ".gz")).foreach {
    case (codecName, extension) =>
      test(s"Output $codecName compressed file") {
        val tempFilePath = createTempVcf

        val ds = spark.read.format(sourceName).option("vcfRowSchema", true).load(TGP)
        ds.write
          .format(sourceName)
          .option("compression", codecName)
          .save(tempFilePath.toString)

        val filesWritten =
          Files.list(tempFilePath).collect(Collectors.toList[Path]).asScala.map(_.toString)
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
