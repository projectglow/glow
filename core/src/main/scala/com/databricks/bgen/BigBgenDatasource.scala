package com.databricks.bgen

import java.io.ByteArrayOutputStream

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.sources.DataSourceRegister

import com.databricks.sql.BigFileDatasource

class BigBgenDatasource extends BigFileDatasource with DataSourceRegister {

  override def shortName(): String = "com.databricks.bigbgen"

  override def serializeDataFrame(options: Map[String, String], data: DataFrame): RDD[Array[Byte]] =
    BigBgenDatasource.serializeDataFrame(options, data)
}

object BigBgenDatasource {

  val BITS_PER_PROB_KEY = "bitsPerProbability"
  val BITS_PER_PROB_DEFAULT_VALUE = "16"

  val MAX_PLOIDY_KEY = "maximumInferredPloidy"
  val MAX_PLOIDY_VALUE = "10"

  val DEFAULT_PLOIDY_KEY = "defaultInferredPloidy"
  val DEFAULT_PLOIDY_VALUE = "2"

  val DEFAULT_PHASING_KEY = "defaultInferredPhasing"
  val DEFAULT_PHASING_VALUE = "false"

  def serializeDataFrame(options: Map[String, String], data: DataFrame): RDD[Array[Byte]] = {

    val dSchema = data.schema
    val numVariants = data.count
    val bitsPerProb = options.getOrElse(BITS_PER_PROB_KEY, BITS_PER_PROB_DEFAULT_VALUE).toInt
    val maxPloidy = options.getOrElse(MAX_PLOIDY_KEY, MAX_PLOIDY_VALUE).toInt
    val defaultPloidy = options.getOrElse(DEFAULT_PLOIDY_KEY, DEFAULT_PLOIDY_VALUE).toInt
    val defaultPhasing = options.getOrElse(DEFAULT_PHASING_KEY, DEFAULT_PHASING_VALUE).toBoolean

    data.queryExecution.toRdd.mapPartitionsWithIndex {
      case (idx, it) =>
        val baos = new ByteArrayOutputStream()

        val writeHeader = idx == 0
        val writer = new BgenRecordWriter(
          baos,
          dSchema,
          writeHeader,
          numVariants,
          bitsPerProb,
          maxPloidy,
          defaultPloidy,
          defaultPhasing
        )

        it.foreach { row =>
          writer.write(row)
        }

        writer.close()
        Iterator(baos.toByteArray)
    }
  }
}
