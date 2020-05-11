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

import java.io.{BufferedReader, InputStreamReader}

import scala.collection.JavaConverters._

import com.google.common.io.LittleEndianDataInputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{ArrayType, StructType}

import io.projectglow.common._
import io.projectglow.sql.GlowBaseTest

class BgenReaderSuite extends GlowBaseTest {

  val sourceName = "bgen"
  private val testRoot = s"$testDataHome/bgen"

  private def iterateFile(path: String): Seq[BgenRow] = {
    val p = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val baseStream = fs.open(p)
    val stream = new LittleEndianDataInputStream(baseStream)
    val header = new BgenHeaderReader(stream).readHeader()
    val iterator = new BgenFileIterator(header, stream, baseStream, 0, fs.getFileStatus(p).getLen)
    iterator.init()
    val ret = iterator.toList
    baseStream.close()
    ret
  }

  private def compareBgenToVcf(bgenPath: String, vcfPath: String): Unit = {
    val sess = spark
    import sess.implicits._

    val bgen = iterateFile(bgenPath)
      .sortBy(r => (r.contigName, r.start))
      .map(r => r.copy(names = r.names.filter(_.nonEmpty).distinct.sorted))
    val vcf = spark
      .read
      .format("vcf")
      .schema(VCFRow.schema)
      .load(vcfPath)
      .orderBy("contigName", "start")
      .as[VCFRow]
      .collect()
      .toSeq
      .map { r =>
        val names = r.names
        // QCTools incorrectly separates IDs with commas instead of semicolons when exporting to VCF
        r.copy(names = names.flatMap(_.split(",")))
      }

    assert(bgen.size == vcf.size)
    bgen.zip(vcf).foreach {
      case (br, vr) =>
        assert(br.contigName == vr.contigName)
        assert(br.start == vr.start)
        assert(br.end == vr.end)
        assert(br.names == vr.names)
        assert(br.referenceAllele == vr.referenceAllele)
        assert(br.alternateAlleles == vr.alternateAlleles)
        assert(br.genotypes.length == vr.genotypes.length)

        br.genotypes.zip(vr.genotypes).foreach {
          case (bg, vg) =>
            // Note: QCTools inserts a dummy "NA" sample ID if absent when exporting to VCF
            assert(bg.sampleId == vg.sampleId || vg.sampleId.get.startsWith("NA"))
            bg.posteriorProbabilities.indices.foreach { i =>
              bg.posteriorProbabilities(i) == vg.posteriorProbabilities.get(i)
            }
        }
    }
  }

  private def getSampleIds(path: String, colIdx: Int = 1): Seq[String] = {
    val p = new Path(path)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val stream = fs.open(p)
    val streamReader = new InputStreamReader(stream)

    val bufferedReader = new BufferedReader(streamReader)
    // The first two (2) lines in a .sample file are header lines
    val sampleIds = bufferedReader
      .lines()
      .skip(2)
      .iterator()
      .asScala
      .map(_.split(" ").apply(colIdx))
      .toList

    stream.close()
    sampleIds
  }

  test("unphased 8 bit") {
    compareBgenToVcf(s"$testRoot/example.8bits.bgen", s"$testRoot/example.8bits.vcf")
  }

  test("unphased 16 bit (with missing samples)") {
    compareBgenToVcf(s"$testRoot/example.16bits.bgen", s"$testRoot/example.16bits.vcf")
  }

  test("unphased 32 bit") {
    compareBgenToVcf(s"$testRoot/example.32bits.bgen", s"$testRoot/example.32bits.vcf")
  }

  test("phased") {
    compareBgenToVcf(s"$testRoot/phased.16bits.bgen", s"$testRoot/phased.16bits.vcf")
  }

  test("complex 16 bit") {
    compareBgenToVcf(s"$testRoot/complex.16bits.bgen", s"$testRoot/complex.16bits.vcf")
  }

  test("skip entire file") {
    val p = new Path(s"$testRoot/example.16bits.bgen")
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val baseStream = fs.open(p)
    val stream = new LittleEndianDataInputStream(baseStream)
    val header = new BgenHeaderReader(stream).readHeader()
    val iterator = new BgenFileIterator(
      header,
      stream,
      baseStream,
      fs.getFileStatus(p).getLen,
      fs.getFileStatus(p).getLen
    )
    iterator.init()
    assert(!iterator.hasNext) // should skip entire file
  }

  test("skip to last record") {
    val p = new Path(s"$testRoot/complex.16bits.bgen")
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val baseStream = fs.open(p)
    val stream = new LittleEndianDataInputStream(baseStream)
    val header = new BgenHeaderReader(stream).readHeader()
    val iterator = new BgenFileIterator(
      header,
      stream,
      baseStream,
      774L, // last record start position
      fs.getFileStatus(p).getLen
    )
    iterator.init()
    assert(iterator.toSeq.size == 1)
  }

  test("read with spark") {
    val sess = spark
    import sess.implicits._
    val path = s"$testRoot/example.16bits.bgen"
    val fromSpark = spark
      .read
      .format(sourceName)
      .load(path)
      .orderBy("contigName", "start")
      .as[BgenRow]
      .collect()
      .toSeq
    val direct = iterateFile(path).sortBy(r => (r.contigName, r.start))

    assert(fromSpark.size == direct.size)
    assert(fromSpark == direct)
  }

  test("read with spark (no index)") {
    val path = s"$testRoot/example.16bits.noindex.bgen"
    val fromSpark = spark
      .read
      .format(sourceName)
      .load(path)
      .orderBy("contigName", "start")
      .collect()

    assert(fromSpark.length == iterateFile(path).size)
  }

  test("read only certain fields") {
    val sess = spark
    import sess.implicits._

    val path = s"$testRoot/example.16bits.bgen"
    val allele = spark
      .read
      .format(sourceName)
      .load(path)
      .orderBy("start")
      .select("referenceAllele")
      .as[String]
      .first

    assert(allele == "A")
  }

  case class WeirdSchema(animal: String)
  test("be permissive if schema includes fields that can't be derived from BGEN") {
    val path = s"$testRoot/example.16bits.noindex.bgen"
    spark
      .read
      .schema(ScalaReflection.schemaFor[WeirdSchema].dataType.asInstanceOf[StructType])
      .format(sourceName)
      .load(path)
      .rdd
      .count() // No error expected
  }

  test("Sample IDs present if .sample file is provided") {
    val sess = spark
    import sess.implicits._

    val basePath = s"$testRoot/example.16bits.oxford"
    val ds = spark
      .read
      .option("sampleFilePath", s"$basePath.sample")
      .option("sampleIdColumn", "ID_2")
      .format(sourceName)
      .load(s"$basePath.bgen")
      .as[BgenRow]
      .head

    val sampleIds = getSampleIds(s"$basePath.sample")
    assert(ds.genotypes.map(_.sampleId.get) == sampleIds)
  }

  test("Sample IDs present if no sample column provided but matches default") {
    val sess = spark
    import sess.implicits._

    val basePath = s"$testRoot/example.16bits.oxford"
    val ds = spark
      .read
      .option("sampleFilePath", s"$basePath.sample")
      .format(sourceName)
      .load(s"$basePath.bgen")
      .as[BgenRow]
      .head

    val sampleIds = getSampleIds(s"$basePath.sample")
    assert(ds.genotypes.map(_.sampleId.get) == sampleIds)
  }

  test("Returns all sample IDs provided in corrupted .sample file") {
    val sess = spark
    import sess.implicits._

    val basePath = s"$testRoot/example.16bits.oxford"
    assertThrows[IllegalArgumentException](
      spark
        .read
        .option("sampleFilePath", s"$basePath.corrupted.sample")
        .format(sourceName)
        .load(s"$basePath.bgen")
        .as[BgenRow]
        .head
    )
  }

  test("Only uses .sample file if no samples in bgen file") {
    val sess = spark
    import sess.implicits._

    val path = s"$testRoot/example.16bits.bgen"
    val ds = spark
      .read
      .option("sampleFilePath", s"$testRoot/example.fake.sample")
      .format(sourceName)
      .load(path)
      .as[BgenRow]
      .head

    assert(ds.genotypes.forall(!_.sampleId.get.startsWith("fake")))
  }

  test("Throws if wrong provided column name") {
    val sess = spark
    import sess.implicits._

    val basePath = s"$testRoot/example.16bits.oxford"
    assertThrows[IllegalArgumentException](
      spark
        .read
        .option("sampleFilePath", s"$basePath.sample")
        .option("sampleIdColumn", "FAKE")
        .format(sourceName)
        .load(s"$basePath.bgen")
        .as[BgenRow]
        .head
    )
  }

  test("Sample IDs present using provided column name") {
    val sess = spark
    import sess.implicits._

    val ds = spark
      .read
      .option("sampleFilePath", s"$testRoot/example.sample")
      .option("sampleIdColumn", "ID_1")
      .format(sourceName)
      .load(s"$testRoot/example.16bits.oxford.bgen")
      .as[BgenRow]
      .head

    val sampleIds = getSampleIds(s"$testRoot/example.sample", 0)
    assert(ds.genotypes.map(_.sampleId.get) == sampleIds)
  }

  test("Throws if default sample column doesn't match") {
    val sess = spark
    import sess.implicits._

    assertThrows[IllegalArgumentException](
      spark
        .read
        .option("sampleFilePath", s"$testRoot/example.sample")
        .format(sourceName)
        .load(s"$testRoot/example.16bits.oxford.bgen")
        .as[BgenRow]
        .head
    )
  }

  test("Skip non-bgen files") {
    val input = s"$testRoot/example.8bits.*"
    spark.read.format(sourceName).load(input).count() // No error

    // Expect an error because we try to read non-bgen files as bgen
    intercept[SparkException] {
      spark
        .read
        .format(sourceName)
        .option(BgenOptions.IGNORE_EXTENSION_KEY, true)
        .load(input)
        .count()
    }
  }

  private def hasSampleIdField(schema: StructType): Boolean = {
    schema
      .find(_.name == VariantSchemas.genotypesFieldName)
      .get
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]
      .exists(_.name == VariantSchemas.sampleIdField.name)
  }

  test("schema does not include sample id field if there are no ids") {
    val df = spark
      .read
      .format(sourceName)
      .load(s"$testRoot/example.16bits.nosampleids.bgen")

    assert(!hasSampleIdField(df.schema))
  }

  test("schema includes sample id if at least one file has ids") {
    val df = spark
      .read
      .format(sourceName)
      .load(s"$testRoot/example.16bits*.bgen")
    assert(hasSampleIdField(df.schema))
  }

  test("schema includes sample id if sample id file is provided") {
    val df = spark
      .read
      .option("sampleFilePath", s"$testRoot/example.16bits.oxford.sample")
      .option("sampleIdColumn", "ID_2")
      .format(sourceName)
      .load(s"$testRoot/example.16bits.nosampleids.bgen")
    assert(hasSampleIdField(df.schema))
  }

  test("schema does not include sample ids if `includeSampleIds` is false") {
    val df = spark
      .read
      .format(sourceName)
      .option(CommonOptions.INCLUDE_SAMPLE_IDS, false)
      .load(s"$testRoot/example.16bits*.bgen")
    assert(!hasSampleIdField(df.schema))
  }
}
