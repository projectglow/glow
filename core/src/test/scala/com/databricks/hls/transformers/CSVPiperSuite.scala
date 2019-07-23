/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.hls.transformers

import com.databricks.hls.SparkGenomics
import com.databricks.hls.sql.HLSBaseTest
import org.apache.spark.sql.DataFrame

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

    SparkGenomics.transform(
      "pipe",
      inputDf,
      baseOptions ++ inputDelimiterOption ++ outputDelimiterOption ++ inputHeaderOption ++ outputHeaderOption)
  }

  test("Delimiter and header") {
    val inputDf = spark.read.option("delimiter", " ").option("header", "true").csv(saige)
    val outputDf =
      pipeCsv(inputDf, s"""["sed", "s/:/ /g"]""", Some(":"), Some(" "), Some(true), Some(true))
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

  test("No rows with header") {
    val inputDf =
      spark.read.option("delimiter", " ").option("header", "true").csv(saige).limit(0)
    val outputDf =
      pipeCsv(inputDf, s"""["cat", "-"]""", Some(" "), Some(" "), Some(true), Some(true))

    assert(outputDf.schema == inputDf.schema)
    assert(outputDf.count == 0)
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

  test("No rows and no header") {
    val inputDf =
      spark.read.option("delimiter", ",").option("header", "false").csv(csv).limit(0)
    val outputDf = pipeCsv(inputDf, s"""["cat", "-"]""", None, None, None, None)

    assert(outputDf.schema == inputDf.schema)
    assert(outputDf.count == 0)
  }
}
