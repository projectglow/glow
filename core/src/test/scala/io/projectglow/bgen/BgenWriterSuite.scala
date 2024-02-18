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

package io.projectglow.bgen

import java.io.{File, FileInputStream}
import java.nio.file.Files

import com.google.common.io.LittleEndianDataInputStream
import org.apache.spark.SparkException

import io.projectglow.common.BgenRow

class BgenWriterSuite extends BgenConverterBaseTest {

  val sourceName = "bigbgen"

  protected def createTempBgen: String = {
    val tempDir = Files.createTempDirectory("bgen")
    tempDir.resolve("test.bgen").toString
  }

  def roundTrip(testBgen: String, bitsPerProb: Int) {
    val sess = spark
    import sess.implicits._

    val newBgenFile = createTempBgen

    val origDs =
      spark.read.format("bgen").schema(BgenRow.schema).load(testBgen).as[BgenRow]
    origDs
      .write
      .option("bitsPerProbability", bitsPerProb)
      .format(sourceName)
      .save(newBgenFile)

    // Check that rows in new file are approximately equal
    val newDs =
      spark.read.format("bgen").schema(BgenRow.schema).load(newBgenFile).as[BgenRow]
    origDs
      .sort("contigName", "start")
      .collect
      .zip(newDs.sort("contigName", "start").collect)
      .foreach { case (or, nr) => checkBgenRowsEqual(or, nr, true, bitsPerProb) }

    // Check that size of files are approximately equal (excluding free data area in header)
    val origStream = new FileInputStream(testBgen)
    val littleEndianOrigStream = new LittleEndianDataInputStream(origStream)
    littleEndianOrigStream.skipBytes(4) // Skip to size of header
    val headerLength = littleEndianOrigStream.readInt()
    val freeDataSize = headerLength - 20
    origStream.close()

    val origFileSize = new File(testBgen).length()
    val newFileSize = new File(newBgenFile).length()
    require(
      origFileSize - freeDataSize ~= newFileSize relTol 0.1,
      s"Orig file size $origFileSize with free data size $freeDataSize, new file size $newFileSize"
    )
  }

  def changeVariantIdAndRsid(variantId: Option[String], rsid: Option[String], testBgen: String) {
    val sess = spark
    import sess.implicits._

    val newBgenFile = createTempBgen

    val origDs = spark
      .read
      .format("bgen")
      .load(testBgen)
      .as[BgenRow]
      .map { br =>
        br.copy(names = Seq(variantId, rsid).flatten)
      }

    origDs
      .write
      .format(sourceName)
      .save(newBgenFile)

    spark
      .read
      .format("bgen")
      .load(newBgenFile)
      .as[BgenRow]
      .collect
      .foreach { br =>
        assert(br.names == Seq(variantId.getOrElse(""), rsid.getOrElse("")))
      }
  }

  def roundTripVcf(
      testBgen: String,
      testVcf: String,
      bitsPerProb: Int,
      defaultPhasing: Option[Boolean] = None) {

    val sess = spark
    import sess.implicits._

    val newBgenFile = createTempBgen

    val bgenDs = spark
      .read
      .format("bgen")
      .schema(BgenRow.schema)
      .load(testBgen)
      .as[BgenRow]
    val origVcfDs = spark
      .read
      .format("vcf")
      .option("includeSampleIds", true)
      .load(testVcf)

    val writer = origVcfDs
      .write
      .option("bitsPerProbability", bitsPerProb)
      .format(sourceName)

    val writerWithDefaultPhasing = if (defaultPhasing.isDefined) {
      writer.option("defaultInferredPhasing", defaultPhasing.get)
    } else {
      writer
    }
    writerWithDefaultPhasing.save(newBgenFile)

    val vcfDs = spark
      .read
      .format("bgen")
      .schema(BgenRow.schema)
      .load(newBgenFile)
      .as[BgenRow]

    bgenDs
      .sort("contigName", "start")
      .collect
      .zip(vcfDs.sort("contigName", "start").collect)
      .foreach { case (br, vr) =>
        checkBgenRowsEqual(br, vr, false, bitsPerProb)
      }
  }

  test("unphased 8 bit") {
    roundTrip(s"$testRoot/example.8bits.bgen", 8)
  }

  test("unphased 16 bit (with missing samples)") {
    roundTrip(s"$testRoot/example.16bits.bgen", 16)
  }

  test("unphased 32 bit") {
    roundTrip(s"$testRoot/example.32bits.bgen", 32)
  }

  test("phased") {
    roundTrip(s"$testRoot/phased.16bits.bgen", 16)
  }

  test("complex 16 bit") {
    roundTrip(s"$testRoot/complex.16bits.bgen", 16)
  }

  test("missing sample IDs") {
    roundTrip(s"$testRoot/example.16bits.oxford.bgen", 16)
  }

  test("missing variant ID and rsid") {
    changeVariantIdAndRsid(None, None, s"$testRoot/example.16bits.bgen")
  }

  test("single name") {
    changeVariantIdAndRsid(Some("fakeId"), None, s"$testRoot/example.16bits.bgen")
  }

  test("invalid bitsPerProb option") {
    val sess = spark
    import sess.implicits._
    assertThrows[SparkException](
      Seq
        .empty[BgenRow]
        .toDF()
        .write
        .option("bitsPerProbability", 2)
        .format(sourceName)
        .save(createTempBgen)
    )
  }

  test("Represent probabilities as int") {
    assert(
      BgenRecordWriter.calculateIntProbabilities(bitsPerProb = 2, Seq(0.5, 0.5)).sorted ==
      Seq(1, 2) // Unrounded: 1.5, 1.5
    )
    assert(
      BgenRecordWriter.calculateIntProbabilities(bitsPerProb = 8, Seq(0.99, 0.01)) ==
      Seq(252, 3) // Unrounded: 252.45, 2.55
    )
    assert(
      BgenRecordWriter.calculateIntProbabilities(bitsPerProb = 16, Seq(0.605, 0.283, 0.122)) ==
      Seq(39649, 18547, 7995) // Unrounded: 39648.675, 18546.405, 7995.27
    )
    assert(
      BgenRecordWriter.calculateIntProbabilities(bitsPerProb = 32, Seq(0.23, 0.27, 0.16, 0.34)) ==
      Seq(987842478, 1159641170, 687194767, 1460288881)
      // Unrounded: 987842477.85, 1159641169.65, 687194767.2, 1460288880.3
    )
  }

  test("Probability block sizes") {
    assert(
      BgenRecordWriter.getProbabilityBlockSize(
        GenotypeCharacteristics(numAlleles = 3, phased = false, ploidy = 3)
      ) == ProbabilityBlockSize(10, 1)
    )

    assert(
      BgenRecordWriter.getProbabilityBlockSize(
        GenotypeCharacteristics(numAlleles = 4, phased = true, ploidy = 2)
      ) == ProbabilityBlockSize(4, 2)
    )
  }

  test("Empty file") {
    val sess = spark
    import sess.implicits._

    val newBgenFile = createTempBgen

    spark
      .sparkContext
      .emptyRDD[BgenRow]
      .toDS
      .write
      .format(sourceName)
      .save(newBgenFile)

    val rewrittenDs = spark
      .read
      .format("bgen")
      .load(newBgenFile)
    assert(rewrittenDs.collect.isEmpty)
  }

  test("unphased 8 bit VCF") {
    roundTripVcf(s"$testRoot/example.8bits.bgen", s"$testRoot/example.8bits.vcf", 8)
  }

  test("unphased 16 bit (with missing samples) VCF") {
    roundTripVcf(s"$testRoot/example.16bits.bgen", s"$testRoot/example.16bits.vcf", 16)
  }

  test("unphased 32 bit VCF") {
    roundTripVcf(s"$testRoot/example.32bits.bgen", s"$testRoot/example.32bits.vcf", 32)
  }

  test("phased VCF") {
    roundTripVcf(s"$testRoot/phased.16bits.bgen", s"$testRoot/phased.16bits.vcf", 16, Some(true))
  }

  test("No genotype") {
    val sess = spark
    import sess.implicits._

    val newBgenFile = createTempBgen

    val noGtRow = BgenRow("chr1", 10, 11, Nil, "A", Seq("T"), Nil)
    Seq(noGtRow)
      .toDS
      .write
      .format("bigbgen")
      .save(newBgenFile)
    val rewrittenDs = spark
      .read
      .format("bgen")
      .schema(BgenRow.schema)
      .load(newBgenFile)
      .as[BgenRow]
    assert(rewrittenDs.collect.head.genotypes.isEmpty)
  }
}
