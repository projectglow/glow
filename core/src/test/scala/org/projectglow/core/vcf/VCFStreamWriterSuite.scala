package org.projectglow.core.vcf

import java.io.{ByteArrayOutputStream, StringReader}

import scala.collection.JavaConverters._

import htsjdk.tribble.TribbleException.InvalidHeader
import htsjdk.variant.variantcontext.{Allele, GenotypeBuilder, VariantContextBuilder}
import htsjdk.variant.vcf.{VCFCodec, VCFHeader, VCFHeaderLine}
import org.apache.commons.io.IOUtils
import org.bdgenomics.adam.rdd.VCFMetadataLoader

import org.projectglow.core.sql.HLSBaseTest

class VCFStreamWriterSuite extends HLSBaseTest {
  val refA: Allele = Allele.create("A", true)
  val altT: Allele = Allele.create("T", false)

  def getHeaderLines(vcf: String): Set[VCFHeaderLine] = {
    val header = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, vcf)
    header.getMetaDataInInputOrder.asScala.toSet
  }

  test("If not provided, infer from row") {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(stream, VCFRowHeaderLines.allHeaderLines.toSet, None, true)

    val gtA = new GenotypeBuilder("SampleA").alleles(Seq(refA, refA).asJava).make
    val gtB = new GenotypeBuilder("SampleB").alleles(Seq(refA, refA).asJava).make
    val vc = new VariantContextBuilder().chr("1").alleles("A").genotypes(gtA, gtB).make

    writer.write(vc)
    writer.close()

    val outputVcf = stream.toString
    val outputHeader = VCFFileWriter.parseHeaderFromString(outputVcf)
    assert(outputHeader.getGenotypeSamples.asScala == Seq("SampleA", "SampleB"))
  }

  test("If provided, don't infer from row") {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(
      stream,
      VCFRowHeaderLines.allHeaderLines.toSet,
      Some(Seq("SampleA", "SampleB")),
      true)

    val gt = new GenotypeBuilder("SampleC").make
    val vc = new VariantContextBuilder().chr("1").alleles("A").genotypes(gt).make

    writer.write(vc)
    writer.close()

    val outputVcf = stream.toString
    val outputHeader = VCFFileWriter.parseHeaderFromString(outputVcf)
    assert(outputHeader.getGenotypeSamples.asScala == Seq("SampleA", "SampleB"))
  }

  test("Replace empty provided sample IDs in header") {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(
      stream,
      VCFRowHeaderLines.allHeaderLines.toSet,
      Some(Seq("SampleA", "", "SampleB", "")),
      true)

    writer.close()

    val outputVcf = stream.toString
    val outputHeader = VCFFileWriter.parseHeaderFromString(outputVcf)
    assert(
      outputHeader.getGenotypeSamples.asScala == Seq("SampleA", "sample_1", "SampleB", "sample_2"))
  }

  test("Replace empty provided sample IDs in rows") {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(
      stream,
      VCFRowHeaderLines.allHeaderLines.toSet,
      Some(Seq("SampleA", "", "SampleB", "")),
      true)

    val gt = new GenotypeBuilder("").alleles(Seq(refA, altT).asJava).make
    val gtA = new GenotypeBuilder("SampleA").alleles(Seq(refA, refA).asJava).make
    val gtB = new GenotypeBuilder("SampleB").alleles(Seq(altT).asJava).make
    val vc = new VariantContextBuilder()
      .chr("1")
      .alleles(Seq(refA, altT).asJava)
      .genotypes(gtA, gt, gtB)
      .make

    writer.write(vc)
    writer.close()

    val stringReader = new StringReader(stream.toString)
    val lineIterator = new LineIteratorImpl(IOUtils.lineIterator(stringReader).asScala)
    val codec = new VCFCodec()
    val header = codec.readActualHeader(lineIterator).asInstanceOf[VCFHeader]
    assert(header.getGenotypeSamples.asScala == Seq("SampleA", "sample_1", "SampleB", "sample_2"))

    val outputVc = codec.decode(lineIterator)
    assert(outputVc.getContig == "1")
    assert(outputVc.getReference == refA)
    assert(outputVc.getAlternateAlleles == Seq(altT).asJava)
    assert(
      outputVc.getGenotypes.getSampleNames.asScala == Set(
        "SampleA",
        "sample_1",
        "SampleB",
        "sample_2"))
    assert(outputVc.getGenotype("SampleA").getAlleles == Seq(refA, refA).asJava)
    assert(outputVc.getGenotype("SampleB").getAlleles == Seq(altT).asJava)
    assert(outputVc.getGenotype("sample_1").getAlleles == Seq(refA, altT).asJava)
    assert(
      outputVc.getGenotype("sample_2").getAlleles == Seq(Allele.NO_CALL, Allele.NO_CALL).asJava)
  }

  test("Replace empty inferred sample IDs in rows") {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(stream, VCFRowHeaderLines.allHeaderLines.toSet, None, true)

    val gt = new GenotypeBuilder("").alleles(Seq(refA, altT).asJava).make
    val gtA = new GenotypeBuilder("SampleA").alleles(Seq(refA, refA).asJava).make
    val gtB = new GenotypeBuilder("SampleB").alleles(Seq(altT).asJava).make
    val vc = new VariantContextBuilder()
      .chr("1")
      .alleles(Seq(refA, altT).asJava)
      .genotypes(gtA, gt, gtB)
      .make

    writer.write(vc)
    writer.close()

    val stringReader = new StringReader(stream.toString)
    val lineIterator = new LineIteratorImpl(IOUtils.lineIterator(stringReader).asScala)
    val codec = new VCFCodec()
    val header = codec.readActualHeader(lineIterator).asInstanceOf[VCFHeader]
    assert(header.getGenotypeSamples.asScala == Seq("SampleA", "sample_1", "SampleB"))

    val outputVc = codec.decode(lineIterator)
    assert(outputVc.getContig == "1")
    assert(outputVc.getReference == refA)
    assert(outputVc.getAlternateAlleles == Seq(altT).asJava)
    assert(outputVc.getGenotypes.getSampleNames.asScala == Set("SampleA", "sample_1", "SampleB"))
    assert(outputVc.getGenotype("SampleA").getAlleles == Seq(refA, refA).asJava)
    assert(outputVc.getGenotype("SampleB").getAlleles == Seq(altT).asJava)
    assert(outputVc.getGenotype("sample_1").getAlleles == Seq(refA, altT).asJava)
  }

  test("Missing samples") {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(stream, VCFRowHeaderLines.allHeaderLines.toSet, None, true)

    assertThrows[IllegalStateException](writer.close())
  }

  test("Don't write header with VC if told not to") {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(
      stream,
      VCFRowHeaderLines.allHeaderLines.toSet,
      Some(Seq("SampleA", "SampleB")),
      false)
    val vc = new VariantContextBuilder().chr("1").alleles("A").make

    writer.write(vc)
    writer.close()

    val stringReader = new StringReader(stream.toString)
    val lineIterator = new LineIteratorImpl(IOUtils.lineIterator(stringReader).asScala)
    val codec = new VCFCodec()
    assertThrows[InvalidHeader](codec.readActualHeader(lineIterator).asInstanceOf[VCFHeader])
  }

  test("Don't write header for empty stream if told not to") {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(
      stream,
      VCFRowHeaderLines.allHeaderLines.toSet,
      Some(Seq("SampleA", "SampleB")),
      false)

    writer.close()
    assert(stream.size == 0)
  }
}
