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

import java.io.{ByteArrayOutputStream, StringReader}

import scala.collection.JavaConverters._

import htsjdk.tribble.TribbleException.InvalidHeader
import htsjdk.variant.variantcontext.{Allele, GenotypeBuilder, VariantContextBuilder}
import htsjdk.variant.vcf.{VCFCodec, VCFHeader, VCFHeaderLine}
import org.apache.commons.io.IOUtils

import io.projectglow.sql.GlowBaseTest

class VCFStreamWriterSuite extends GlowBaseTest {
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
    val outputHeader = VCFHeaderUtils.parseHeaderFromString(outputVcf)
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
    val outputHeader = VCFHeaderUtils.parseHeaderFromString(outputVcf)
    assert(outputHeader.getGenotypeSamples.asScala == Seq("SampleA", "SampleB"))
  }

  test("Throw if find new samples when inferring from rows") {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(stream, VCFRowHeaderLines.allHeaderLines.toSet, None, true)

    val gtA = new GenotypeBuilder("SampleA").alleles(Seq(refA, altT).asJava).make
    val gtB = new GenotypeBuilder("SampleB").alleles(Seq(refA, refA).asJava).make
    val gtC = new GenotypeBuilder("SampleC").alleles(Seq(altT).asJava).make
    val vcAB = new VariantContextBuilder()
      .chr("1")
      .alleles(Seq(refA, altT).asJava)
      .genotypes(gtA, gtB)
      .make
    val vcABC = new VariantContextBuilder()
      .chr("2")
      .alleles(Seq(refA, altT).asJava)
      .genotypes(gtA, gtB, gtC)
      .make

    writer.write(vcAB)
    val e = intercept[IllegalArgumentException] { writer.write(vcABC) }
    assert(e.getMessage.contains("Inferred VCF header is missing samples"))
    writer.close()
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
