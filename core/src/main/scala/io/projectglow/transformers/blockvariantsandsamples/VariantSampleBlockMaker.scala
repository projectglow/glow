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

package io.projectglow.transformers.blockvariantsandsamples

import io.projectglow.common.GlowLogging
import io.projectglow.common.VariantSchemas._
import io.projectglow.functions._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType}

private[projectglow] object VariantSampleBlockMaker extends GlowLogging {

  def filterOneDistinctValue(df: DataFrame): DataFrame = {
    logger.info("Filtering variants whose values are all the same.")
    df.filter(size(array_distinct(col(valuesField.name))) > 1)
  }

  def makeSampleBlocks(df: DataFrame, sampleBlockCount: Int): DataFrame = {
    df.withColumn(
        "fractionalSampleBlockSize",
        size(col(valuesField.name)) / sampleBlockCount
      )
      .withColumn(
        sampleBlockIdField.name,
        explode(
          sequence(
            lit(1),
            lit(sampleBlockCount)
          ).cast(ArrayType(StringType))
        )
      )
      .withColumn(
        valuesField.name,
        expr(
          s"""slice(
             |   ${valuesField.name},
             |   round((${sampleBlockIdField.name} - 1) * fractionalSampleBlockSize) + 1,
             |   round(${sampleBlockIdField.name} * fractionalSampleBlockSize) - round((${sampleBlockIdField.name} - 1) * fractionalSampleBlockSize)
             |)""".stripMargin
        )
      )
  }

  def makeVariantAndSampleBlocks(
      variantDf: DataFrame,
      variantsPerBlock: Int,
      sampleBlockCount: Int): DataFrame = {
    val windowSpec = Window
      .partitionBy(contigNameField.name, sampleBlockIdField.name)
      .orderBy(startField.name, refAlleleField.name, alternateAllelesField.name)

    val baseDf = filterOneDistinctValue(variantDf)
      .withColumn(
        sortKeyField.name,
        col(startField.name).cast(IntegerType)
      )
      .withColumn(
        headerField.name,
        concat_ws(
          ":",
          col(contigNameField.name),
          col(startField.name),
          col(refAlleleField.name),
          col(alternateAllelesField.name)
        )
      )
      .withColumn(
        "stats",
        subset_struct(
          array_summary_stats(
            col(valuesField.name)
          ),
          "mean",
          "stdDev"
        )
      )
      .withColumn(
        meanField.name,
        col("stats.mean")
      )
      .withColumn(
        stdDevField.name,
        col("stats.stdDev")
      )

    makeSampleBlocks(baseDf, sampleBlockCount)
      .withColumn(
        sizeField.name,
        size(col(valuesField.name))
      )
      .withColumn(
        headerBlockIdField.name,
        concat_ws(
          "_",
          lit("chr"),
          col(contigNameField.name),
          lit("block"),
          ((row_number().over(windowSpec) - 1) / variantsPerBlock).cast(IntegerType)
        )
      )
      .select(
        col(headerField.name),
        col(sizeField.name),
        col(valuesField.name),
        col(headerBlockIdField.name),
        col(sampleBlockIdField.name),
        col(sortKeyField.name),
        col(meanField.name),
        col(stdDevField.name)
      )
  }
}
