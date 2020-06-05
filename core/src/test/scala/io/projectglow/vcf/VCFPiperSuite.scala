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

import java.nio.file.Files

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.{SparkException, TaskContext}

import io.projectglow.Glow
import io.projectglow.common.VCFRow
import io.projectglow.transformers.pipe.ProcessHelper

class VCFPiperSuite extends VCFConverterBaseTest {
  lazy val sess = spark
  private val na12878 = s"$testDataHome/NA12878_21_10002403.vcf"
  private val TGP = s"$testDataHome/1000genomes-phase3-1row.vcf"

  override def afterEach(): Unit = {
    Glow.transform("pipe_cleanup", spark.emptyDataFrame)

    // Check that all pipe transformer threads are cleaned up
    eventually {
      assert(
        !Thread
          .getAllStackTraces
          .asScala
          .keySet
          .exists(_.getName.startsWith(ProcessHelper.STDIN_WRITER_THREAD_PREFIX))
      )
      assert(
        !Thread
          .getAllStackTraces
          .asScala
          .keySet
          .exists(_.getName.startsWith(ProcessHelper.STDERR_READER_THREAD_PREFIX))
      )
    }

    super.afterEach()
  }

  private def readVcf(vcf: String): DataFrame = {
    spark
      .read
      .format("vcf")
      .option("flattenInfoFields", true)
      .load(vcf)
  }

  def pipeScript(vcf: String, script: String): (DataFrame, DataFrame) = {
    val inputDf = readVcf(vcf)
    val options = Map(
      "inputFormatter" -> "vcf",
      "outputFormatter" -> "vcf",
      "inVcfHeader" -> "infer",
      "cmd" -> s"""["$script"]""")
    val outputDf = Glow.transform("pipe", inputDf, options)

    (inputDf, outputDf)
  }

  test("Cat") {
    val (inputDf, outputDf) = pipeScript(na12878, "cat")
    val inputRows = inputDf.collect().toSeq
    val outputRows = outputDf.collect().toSeq
    assert(inputRows == outputRows)
  }

  test("Prepend chr") {
    val (_, df) = pipeScript(na12878, s"$testDataHome/vcf/scripts/prepend-chr.sh")
    df.cache()

    import sess.implicits._

    // Prepends chr
    val distinctContigNames = df.select("contigName").as[String].distinct.collect
    assert(distinctContigNames.length == 1 && distinctContigNames.head == "chr21")

    // Include sample names
    val sampleSeq = df.select("genotypes.sampleId").as[Seq[String]].head
    assert(sampleSeq == Seq("NA12878"))

    // Flattens INFO fields
    val sorSeq = df.select("INFO_SOR").as[Double].collect
    assert(sorSeq.min ~== 0.551 relTol 0.2)

    df.unpersist()
  }

  test("Remove INFO") {
    val (_, df) = pipeScript(na12878, s"$testDataHome/vcf/scripts/remove-info.sh")

    assert(!df.schema.fieldNames.exists(_.startsWith("INFO_")))
  }

  test("Remove non-header rows") {
    val (inputDf, outputDf) = pipeScript(na12878, s"$testDataHome/vcf/scripts/remove-rows.sh")

    assert(inputDf.schema == outputDf.schema)
    assert(outputDf.isEmpty)
  }

  private val baseTextOptions = Map("inputFormatter" -> "vcf", "outputFormatter" -> "text")
  test("environment variables") {
    import sess.implicits._

    val options = baseTextOptions ++ Map(
        "in_vcfHeader" -> "infer",
        "cmd" -> """["printenv"]""",
        "env_animal" -> "monkey",
        "env_a" -> "b",
        "env_c" -> "D",
        "envE" -> "F")
    val df = readVcf(na12878)
    val output = Glow
      .transform("pipe", df, options)
      .as[String]
      .collect()
      .toSeq
    assert(output.contains("animal=monkey"))
    assert(output.contains("a=b"))
    assert(output.contains("c=D"))
    assert(output.contains("e=F"))
  }

  test("empty partition") {
    val df = readVcf(na12878).repartition(8)
    assert(df.count == 4)

    val options = baseTextOptions ++ Map(
        "cmd" -> """["wc", "-l"]""",
        "inVcfHeader" -> na12878
      )
    val output = Glow.transform("pipe", df, options)
    assert(output.count() == 4)
    assert(output.rdd.getNumPartitions == 8)
  }

  test("empty partition and missing samples") {
    val df = readVcf(na12878).repartition(8)
    assert(df.count == 4)

    val options = baseTextOptions ++ Map("cmd" -> """["wc", "-l"]""", "in_vcf_header" -> "infer")
    val output = Glow.transform("pipe", df, options)

    import sess.implicits._
    val outputStrs =
      output.selectExpr("cast(trim(text) as int) as num_lines").as[Int].collect().toSeq
    // Inferred header contains 23 rows, and each partition contains 1 row with data
    assert(outputStrs == Seq(24, 24, 24, 24))
    assert(output.rdd.getNumPartitions == 8)
  }

  test("stdin and stderr threads are cleaned up for successful commands") {
    pipeScript(na12878, "cat")
  }

  test("command doesn't exist") {
    val ex = intercept[SparkException] {
      pipeScript(na12878, "totallyfakecommandthatdoesntexist")
    }
    assert(ex.getMessage.contains("No such file or directory"))
  }

  test("no rows") {
    val df = readVcf(na12878).limit(0)
    val options = baseTextOptions ++ Map("inVcfHeader" -> na12878, "cmd" -> s"""["cat", "-"]""")
    val ex = intercept[IllegalStateException] {
      Glow.transform("pipe", df, options)
    }
    assert(ex.getMessage.contains("Cannot infer schema: saw 0 distinct schemas"))
  }

  test("task context is defined in each thread") {
    import sess.implicits._

    val input = spark
      .read
      .format("vcf")
      .schema(VCFRow.schema)
      .load(na12878)
      .as[VCFRow]
    val df = input.map { el =>
      require(TaskContext.get != null)
      el
    }.toDF

    val options = Map(
      "inputFormatter" -> "vcf",
      "outputFormatter" -> "vcf",
      "inVcfHeader" -> na12878,
      "cmd" -> s"""["cat", "-"]""")
    val output = Glow.transform("pipe", df, options)
    assert(output.count() == 4)
  }

  test("missing sample names") {
    import sess.implicits._

    val inputDf = setMissingSampleIds(
      spark
        .read
        .schema(VCFRow.schema)
        .format("vcf")
        .load(TGP)
    )

    val options = Map(
      "inputFormatter" -> "vcf",
      "outputFormatter" -> "vcf",
      "inVcfHeader" -> "infer",
      "cmd" -> s"""["cat", "-"]"""
    )
    val outputDf = Glow.transform("pipe", inputDf.toDF, options)

    inputDf.as[SimpleVcfRow].collect.zip(outputDf.as[SimpleVcfRow].collect).foreach {
      case (vc1, vc2) =>
        var missingSampleIdx = 0
        val gtsWithSampleIds = vc1.genotypes.map { gt =>
          missingSampleIdx += 1
          gt.copy(sampleId = Some(s"sample_$missingSampleIdx"))
        }
        val vc1WithSampleIds = vc1.copy(genotypes = gtsWithSampleIds)
        assert(
          vc1WithSampleIds.equals(vc2),
          s"VC1 $vc1WithSampleIds VC2 $vc2"
        )
    }
  }

  test("input validation stringency") {
    val inputDf = spark
      .read
      .format("vcf")
      .load(TGP)
      .withColumn("INFO_fake", lit("foobar"))

    val options = Map(
      "inputFormatter" -> "vcf",
      "outputFormatter" -> "vcf",
      "inVcfHeader" -> TGP,
      "inValidationStringency" -> "STRICT",
      "cmd" -> s"""["cat", "-"]"""
    )
    assertThrows[IllegalArgumentException](Glow.transform("pipe", inputDf, options))
  }

  test("output validation stringency") {
    val row = Seq("1", "1", "id", "C", "T,GT", "1", ".", "AC=monkey").mkString("\t")

    val file = Files.createTempFile("test-vcf", ".vcf")
    val header =
      s"""##fileformat=VCFv4.2
         |##INFO=<ID=AC,Number=1,Type=Integer,Description="">
         |#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
        """.stripMargin
    FileUtils.writeStringToFile(file.toFile, header + row)

    val inputDf = spark
      .read
      .format("vcf")
      .load(na12878)

    val options = Map(
      "inputFormatter" -> "vcf",
      "outputFormatter" -> "vcf",
      "inVcfHeader" -> "infer",
      "outValidationStringency" -> "STRICT",
      "cmd" -> s"""["cat", "$file"]"""
    )
    val ex = intercept[SparkException] {
      Glow.transform("pipe", inputDf, options)
    }
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
    assert(ex.getMessage.contains("Could not parse INFO field AC"))
  }

  test("throw if input formatter fails") {
    val inputDf = spark
      .read
      .format("vcf")
      .load(TGP)
      .drop("contigName")

    val options = Map(
      "inputFormatter" -> "vcf",
      "outputFormatter" -> "vcf",
      "inVcfHeader" -> "infer",
      "inValidationStringency" -> "STRICT",
      "cmd" -> s"""["cat"]"""
    )

    val e = intercept[SparkException](Glow.transform("pipe", inputDf, options))
    assert(e.getCause.isInstanceOf[IllegalArgumentException])
    assert(e.getCause.getMessage.contains("Could not build variant context: Contig cannot be null"))
  }

  test("pass command as a Seq") {
    val inputDf = readVcf(TGP)
    val options = Map(
      "inputFormatter" -> "vcf",
      "outputFormatter" -> "vcf",
      "inVcfHeader" -> "infer",
      "cmd" -> Seq("cat", "-u", "-s"))
    val outputDf = Glow.transform("pipe", inputDf, options)
    val inputRows = inputDf.collect().toSeq
    val outputRows = outputDf.collect().toSeq
    assert(inputRows == outputRows)
  }
}

case class SimpleVcfRow(contigName: String, start: Long, genotypes: Seq[SimpleGenotype])

case class SimpleGenotype(
    sampleId: Option[String],
    phased: Option[Boolean],
    calls: Option[Seq[Int]])
