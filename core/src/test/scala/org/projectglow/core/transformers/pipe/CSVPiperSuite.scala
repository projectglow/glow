/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package org.projectglow.core.transformers.pipe

import com.databricks.hls.DBGenomics
import com.databricks.hls.sql.HLSBaseTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField}
import org.projectglow.DBGenomics
import org.projectglow.core.Glow
import org.projectglow.core.sql.HLSBaseTest
import org.projectglow.sql.HLSBaseTest

class CSVPiperSuite extends HLSBaseTest {
  private val saige = s"$testDataHome/saige_output.txt"
  private val csv = s"$testDataHome/no_header.csv"

  def pipeCsv(
      inputDf: DataFrame,
      cmd: String,
      inputDelimiter: Option[String],
      outputDelimiter: Option[String],
      inputHeader: Option[Boolean],
      outputHeader: Option[Boolean]): DataFrame = {

    val baseOptions =
      Map("inputFormatter" -> "csv", "outputFormatter" -> "csv", "cmd" -> cmd)

    val inputDelimiterOption = if (inputDelimiter.isDefined) {
      Map("in_delimiter" -> inputDelimiter.get)
    } else {
      Map.empty
    }
    val outputDelimiterOption = if (outputDelimiter.isDefined) {
      Map("out_delimiter" -> outputDelimiter.get)
    } else {
      Map.empty
    }

    val inputHeaderOption = if (inputHeader.isDefined) {
      Map("in_header" -> inputHeader.get.toString)
    } else {
      Map.empty
    }

    val outputHeaderOption = if (outputHeader.isDefined) {
      Map("out_header" -> outputHeader.get.toString)
    } else {
      Map.empty
    }

    Glow.transform(
      "pipe",
      inputDf,
      baseOptions ++ inputDelimiterOption ++ outputDelimiterOption ++ inputHeaderOption ++ outputHeaderOption)
  }

  test("Delimiter and header") {
    val inputDf = spark.read.option("delimiter", " ").option("header", "true").csv(saige)
    val outputDf =
      pipeCsv(inputDf, """["sed", "s/:/ /g"]""", Some(":"), Some(" "), Some(true), Some(true))
    assert(outputDf.schema == inputDf.schema)
    assert(
      outputDf.orderBy("CHR", "POS").collect.toSeq == inputDf.orderBy("CHR", "POS").collect.toSeq)
  }

  test("Some empty partitions with header") {
    val inputDf =
      spark.read.option("delimiter", " ").option("header", "true").csv(saige).repartition(20)
    val outputDf =
      pipeCsv(inputDf, s"""["cat", "-"]""", Some(" "), Some(" "), Some(true), Some(true))
    assert(outputDf.schema == inputDf.schema)
    assert(
      outputDf.orderBy("CHR", "POS").collect.toSeq == inputDf.orderBy("CHR", "POS").collect.toSeq)
  }

  test("Single row with header") {
    val inputDf =
      spark.read.option("delimiter", " ").option("header", "true").csv(saige).limit(1)
    val outputDf =
      pipeCsv(inputDf, s"""["cat", "-"]""", Some(" "), Some(" "), Some(true), Some(true))

    assert(outputDf.schema == inputDf.schema)
    assert(outputDf.collect.toSeq == inputDf.collect.toSeq)
  }

  test("No rows with header") {
    val inputDf =
      spark.read.option("delimiter", " ").option("header", "true").csv(saige).limit(0)
    val outputDf =
      pipeCsv(inputDf, s"""["cat", "-"]""", Some(" "), Some(" "), Some(true), Some(true))
    assert(outputDf.schema == inputDf.schema)
    assert(outputDf.isEmpty)
  }

  test("Default options: comma delimiter and no header") {
    val inputDf = spark.read.option("delimiter", ",").option("header", "false").csv(csv)
    val outputDf = pipeCsv(inputDf, s"""["cat", "-"]""", None, None, None, None)

    assert(outputDf.schema == inputDf.schema)
    assert(outputDf.orderBy("_c0").collect.toSeq == inputDf.orderBy("_c0").collect.toSeq)
  }

  test("Some empty partitions without header") {
    val inputDf =
      spark.read.option("delimiter", ",").option("header", "false").csv(csv).repartition(20)
    val outputDf = pipeCsv(inputDf, s"""["cat", "-"]""", None, None, None, None)

    assert(outputDf.schema == inputDf.schema)
    assert(outputDf.orderBy("_c0").collect.toSeq == inputDf.orderBy("_c0").collect.toSeq)
  }

  test("Single row and no header") {
    val inputDf =
      spark.read.option("delimiter", ",").option("header", "false").csv(csv).limit(1)
    val outputDf = pipeCsv(inputDf, s"""["cat", "-"]""", None, None, None, None)

    assert(outputDf.schema == inputDf.schema)
    assert(outputDf.collect.toSeq == inputDf.collect.toSeq)
  }

  test("No rows and no header") {
    val inputDf =
      spark.read.option("delimiter", ",").option("header", "false").csv(csv).limit(0)
    assertThrows[IllegalStateException](
      pipeCsv(inputDf, s"""["cat", "-"]""", None, None, None, None))
  }

  test("GWAS") {
    val na12878 = s"$testDataHome/NA12878_21_10002403.vcf"
    val input =
      spark.read.format("vcf").load(na12878)
    val options = Map(
      "inputFormatter" -> "vcf",
      "outputFormatter" -> "csv",
      "in_vcfHeader" -> na12878,
      "out_header" -> "true",
      "out_delimiter" -> " ",
      "cmd" -> s"""["$testDataHome/vcf/scripts/gwas.sh"]"""
    )
    val outputDf = Glow.transform("pipe", input, options)
    assert(
      outputDf.schema.fields.toSeq == Seq(
        StructField("CHR", StringType, nullable = true),
        StructField("POS", StringType, nullable = true),
        StructField("pValue", StringType, nullable = true)))
    assert(outputDf.count == input.count)
    assert(outputDf.filter("pValue = 0.5").count == outputDf.count)
  }

  test("Gene-based GWAS") {
    val na12878 = s"$testDataHome/NA12878_21_10002403.vcf"
    val input =
      spark.read.format("vcf").load(na12878)
    val options = Map(
      "inputFormatter" -> "vcf",
      "outputFormatter" -> "csv",
      "in_vcfHeader" -> na12878,
      "out_header" -> "true",
      "out_delimiter" -> " ",
      "cmd" -> s"""["python", "$testDataHome/vcf/scripts/gwas-region.py", "$testDataHome/vcf/scripts/group_file.txt"]"""
    )
    val outputDf = Glow.transform("pipe", input, options)
    assert(
      outputDf.schema.fields.toSeq == Seq(
        StructField("Gene", StringType, nullable = true),
        StructField("pValue", StringType, nullable = true)))
    assert(outputDf.count == 2)
    assert(outputDf.filter("pValue = 0.5").count == outputDf.count)
  }

  test("Big file") {
    val inputDf = spark.read.text(s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf")
    val outputDf = Glow.transform(
      "pipe",
      inputDf,
      Map("inputFormatter" -> "text", "outputFormatter" -> "csv", "cmd" -> """["cat", "-"]"""))
    assert(outputDf.count() == 1103)
  }
}
