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
  val headerLines: Set[VCFHeaderLine] = VCFRowHeaderLines.allHeaderLines.toSet
  val actualSampleIds: Seq[String] = Seq("SampleA", "SampleB")

  test("VC to infer from has mixed missing and non-missing") {
    val stream = new ByteArrayOutputStream()
    val writer =
      new VCFStreamWriter(stream, headerLines, None, true)
    val gts = Seq("", "SampleA", "").map { s =>
      new GenotypeBuilder(s).alleles(Seq(refA, altT).asJava).make
    }.asJava
    val vcBuilder = new VariantContextBuilder().chr("1").alleles("A", "T")
    val e = intercept[IllegalArgumentException] {
      writer.write(vcBuilder.genotypes(gts).make)
    }
    assert(e.getMessage.contains("Cannot mix missing and non-missing sample IDs"))
  }

  def checkInfer(
      firstRowSampleIds: Seq[String],
      secondRowSampleIds: Seq[String],
      errorMsg: String): Unit = {
    val stream = new ByteArrayOutputStream()
    val writer =
      new VCFStreamWriter(stream, headerLines, None, true)
    val firstGts = firstRowSampleIds.map { s =>
      new GenotypeBuilder(s).alleles(Seq(refA, altT).asJava).make
    }.asJava
    val vcBuilder = new VariantContextBuilder().chr("1").alleles("A", "T")
    writer.write(vcBuilder.genotypes(firstGts).make)

    val secondGts = secondRowSampleIds.map { s =>
      new GenotypeBuilder(s).alleles(Seq(refA, altT).asJava).make
    }.asJava
    val e = intercept[IllegalArgumentException] {
      writer.write(vcBuilder.genotypes(secondGts).make)
    }
    assert(e.getMessage.contains(errorMsg))
  }

  test("Check for new sample IDs") {
    checkInfer(
      actualSampleIds,
      Seq("SampleC"),
      "Found sample ID in row that was not present in the header")
  }

  test("Saw present sample IDs when inferred missing") {
    checkInfer(Seq("", "", ""), actualSampleIds, "Cannot mix missing and non-missing sample IDs")
  }

  test("Saw present sample IDs when inferred missing, same number of samples") {
    checkInfer(Seq("", ""), actualSampleIds, "Cannot mix missing and non-missing sample IDs")
  }

  test("Number of missing does not match") {
    checkInfer(
      Seq("", "", ""),
      Seq("", ""),
      "Number of genotypes in row does not match number of injected missing header samples.")
  }

  test("Unexpected missing sample ID") {
    checkInfer(actualSampleIds, Seq("", ""), "Cannot mix missing and non-missing sample IDs.")
  }

  test("Don't write header with VC if told not to") {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(
      stream,
      headerLines,
      Some(SampleIdsFromMissing.presentSamples(actualSampleIds)),
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
      headerLines,
      Some(SampleIdsFromMissing.presentSamples(actualSampleIds)),
      false)

    writer.close()
    assert(stream.size == 0)
  }

  test("Empty partition") {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(stream, headerLines, None, true)
    val e = intercept[IllegalStateException] {
      writer.close()
    }
    assert(e.getMessage.contains("Cannot infer header for empty partition"))
  }
}
