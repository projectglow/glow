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

import java.io.ByteArrayOutputStream

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sources.DataSourceRegister

import io.projectglow.common.logging.{HlsEventRecorder, HlsTagValues}
import io.projectglow.sql.BigFileDatasource
import io.projectglow.sql.util.ComDatabricksDataSource

class BigBgenDatasource extends BigFileDatasource with DataSourceRegister {

  override def shortName(): String = "bigbgen"

  override def serializeDataFrame(
      options: Map[String, String],
      data: DataFrame): RDD[Array[Byte]] = {
    BigBgenDatasource.serializeDataFrame(options, data)
  }

}

class ComDatabricksBigBgenDatasource extends BigBgenDatasource with ComDatabricksDataSource

object BigBgenDatasource extends HlsEventRecorder {

  import io.projectglow.common.BgenOptions._

  def parseOptions(options: Map[String, String]): BigBgenOptions = {
    val bitsPerProb = options.getOrElse(BITS_PER_PROB_KEY, BITS_PER_PROB_DEFAULT_VALUE).toInt
    val maxPloidy = options.getOrElse(MAX_PLOIDY_KEY, MAX_PLOIDY_VALUE).toInt
    val defaultPloidy = options.getOrElse(DEFAULT_PLOIDY_KEY, DEFAULT_PLOIDY_VALUE).toInt
    val defaultPhasing = options.getOrElse(DEFAULT_PHASING_KEY, DEFAULT_PHASING_VALUE).toBoolean
    BigBgenOptions(bitsPerProb, maxPloidy, defaultPloidy, defaultPhasing)
  }

  def logWrite(parsedOptions: BigBgenOptions): Unit = {
    val logOptions = Map(
      BITS_PER_PROB_KEY -> parsedOptions.bitsPerProb,
      MAX_PLOIDY_KEY -> parsedOptions.maxPloidy,
      DEFAULT_PLOIDY_KEY -> parsedOptions.defaultPloidy,
      DEFAULT_PHASING_KEY -> parsedOptions.defaultPhasing
    )
    recordHlsEvent(HlsTagValues.EVENT_BGEN_WRITE, logOptions)
  }

  def serializeDataFrame(options: Map[String, String], data: DataFrame): RDD[Array[Byte]] = {
    val dSchema = data.schema
    val numVariants = data.count
    val parsedOptions = parseOptions(options)

    logWrite(parsedOptions)

    data.queryExecution.toRdd.mapPartitionsWithIndex {
      case (idx, it) =>
        val baos = new ByteArrayOutputStream()

        val writeHeader = idx == 0
        val writer = new BgenRecordWriter(
          baos,
          dSchema,
          writeHeader,
          numVariants,
          parsedOptions.bitsPerProb,
          parsedOptions.maxPloidy,
          parsedOptions.defaultPloidy,
          parsedOptions.defaultPhasing
        )

        it.foreach { row =>
          writer.write(row)
        }

        writer.close()
        Iterator(baos.toByteArray)
    }
  }
}

case class BigBgenOptions(
    bitsPerProb: Int,
    maxPloidy: Int,
    defaultPloidy: Int,
    defaultPhasing: Boolean)
