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
import htsjdk.variant.vcf.{VCFCodec, VCFHeader}
import org.apache.commons.io.IOUtils

import io.projectglow.sql.GlowBaseTest

class VCFStreamWriterSuite extends GlowBaseTest {
  val refA: Allele = Allele.create("A", true)
  val altT: Allele = Allele.create("T", false)
  val header =
    new VCFHeader(VCFRowHeaderLines.allHeaderLines.toSet.asJava, Seq("SampleA", "SampleB").asJava)

  def checkOn(sampleIds: Seq[String], replaceSampleIds: Boolean, errorMsg: String): Unit = {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(stream, header, true, replaceSampleIds, true)
    val gts = sampleIds.map { s =>
      new GenotypeBuilder(s).alleles(Seq(refA, altT).asJava).make
    }
    val vc = new VariantContextBuilder().chr("1").alleles("A", "T").genotypes(gts.asJava).make
    val e = intercept[IllegalArgumentException] {
      writer.write(vc)
    }
    assert(e.getMessage.contains(errorMsg))
    writer.close()
  }

  def checkOff(sampleIds: Seq[String], replaceSampleIds: Boolean): Unit = {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(stream, header, false, replaceSampleIds, true)
    val gts = sampleIds.map { s =>
      new GenotypeBuilder(s).alleles(Seq(refA, altT).asJava).make
    }
    val vc = new VariantContextBuilder().chr("1").alleles("A", "T").genotypes(gts.asJava).make
    writer.write(vc)
    writer.close()
  }

  def checkOnAndOff(sampleIds: Seq[String], replaceSampleIds: Boolean, errorMsg: String): Unit = {
    // Check for thrown exception if checkNewSampleIds is true
    checkOn(sampleIds, replaceSampleIds, errorMsg)
    // Should not fail if check if false
    checkOff(sampleIds, replaceSampleIds)
  }

  test("Check for new sample IDs") {
    checkOnAndOff(
      Seq("SampleC"),
      false,
      "Found sample ID in row that was not present in the header")
  }

  test("Check for new sample IDs works for missing sample IDs") {
    checkOnAndOff(
      Seq(""),
      false,
      "Found missing sample ID in row that was not injected in the header")
  }

  test("Number of missing matches") {
    checkOnAndOff(
      Seq(""),
      true,
      "Number of genotypes in row does not match number of injected missing header samples.")
  }

  test("Unexpected present sample ID") {
    checkOnAndOff(Seq("", "SampleC"), true, "Cannot mix missing and non-missing sample IDs.")
  }

  test("Don't write header with VC if told not to") {
    val stream = new ByteArrayOutputStream()
    val writer = new VCFStreamWriter(stream, header, false, false, false)
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
    val writer = new VCFStreamWriter(stream, header, false, false, false)

    writer.close()
    assert(stream.size == 0)
  }
}
