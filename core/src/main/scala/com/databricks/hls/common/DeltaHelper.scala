/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.hls.common

import java.io.FileNotFoundException
import java.nio.file.Paths

import org.apache.hadoop.fs.Path
import com.google.common.annotations.VisibleForTesting
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * Utility functions to help read from Delta when it's available and parquet when it's
 * not (unit tests)
 */
object DeltaHelper {

  // Used if no codec is set via spark.sql.parquet.compression.codec
  // Snappy decompresses efficiently and is default in Spark.
  private val defaultCompressionCodec = CompressionCodecName.SNAPPY.toString.toLowerCase()

  // Used if no block size is set via parquet.block.size in Hadoop Conf
  // lower block sizes are better for selective queries.
  private val defaultBlockSize = 10 * 1024 * 1024

  @VisibleForTesting
  private[common] def getDatasourceFormat(session: SparkSession): String = {
    val conf = session.conf
    val deltaEnv = conf
      .getOption("spark.databricks.delta.preview.enabled")
      .getOrElse("false")
      .toBoolean

    if (deltaEnv) "delta" else "parquet"
  }

  def load(session: SparkSession, path: String): DataFrame = {
    val format = getDatasourceFormat(session)
    session.read.format(format).load(path)
  }

  /**
   * Save this dataset, optionally partitioning by given column.
   * Existing partitions are overwritten with the new data, while new partitions are appended.
   *
   * @param dataset The dataset to save
   * @param path The path to save this dataset
   * @param partitionBy Partitioning Column(s) if applicable
   * @param partitionValues The Partition values that are present in the dataset.
   *                        If the partition values are not known, they will be computed
   *                        by making a pass over the dataset.
   * @param mode SaveMode: can be one of (overwrite, append, ignore, errorIfExists)
   * @tparam T
   */
  def save[T](
      dataset: Dataset[T],
      path: String,
      partitionBy: Option[String] = None,
      partitionValues: Option[Set[_ <: Any]] = None,
      mode: String = "append"): Unit = {

    // Starting Spark 2.3.0, if spark.sql.sources.partitionOverwriteMode = dynamic
    // Spark will follows Hive's mode of overwriting only the partitions in the dataset
    // However, this doesn't work with Delta correctly unless we use replaceWhere
    // since Delta manages its own metadata regarding partition consistency.
    // Delta uses replaceWhere to determine the partitions to overwrite
    // TODO: remove this logic when this is fixed in Delta

    val p = partitionBy.map((_, partitionValues))
    val writer = p match {
      case None => newWriter(dataset, mode)
      case Some((partitionCol, optExistingValues)) =>
        val existingValues =
          optExistingValues.fold[Set[_]](computePartitions(dataset, partitionCol))(identity)

        // string columns need quotes to be correctly resolved
        // this works in general since other columns will be cast into the wider type
        val stringifiedValues = existingValues.map(v => s"'$v'")
        val replaceWhere = s"$partitionCol in (${stringifiedValues.mkString(",")})"
        newWriter(dataset)
          .partitionBy(partitionCol)
          .option("replaceWhere", replaceWhere)
    }

    writer.save(path)
  }

  /**
   * Checks if a Delta or Parquet table exists. If the partition is provided, we check for the
   * existence of only that partition. If not, we check for any data in the table.
   */
  def tableExists[T](
      session: SparkSession,
      basePath: String,
      partition: Option[(String, T)]): Boolean = {
    val format = getDatasourceFormat(session)
    if (format == "delta") {
      val hPath = new Path(basePath)
      val fs = hPath.getFileSystem(session.sparkContext.hadoopConfiguration)
      val pathExists = fs.exists(hPath)

      if (pathExists) {
        if (!fs.listStatus(hPath).isEmpty) {
          val baseDf = session.read.format("delta").load(basePath)
          val df = partition match {
            case None => baseDf
            case Some((partCol, partVal)) =>
              baseDf.where(col(partCol) === partVal)
          }
          !df.isEmpty
        } else {
          false
        }
      } else {
        false
      }
    } else {
      val path = partition match {
        case None => basePath
        case Some((partCol, partVal)) =>
          Paths.get(basePath).resolve(s"$partCol=${partVal.toString}").toString
      }
      DirectoryHelper.directoryWasCommitted(session, path)
    }
  }

  private def computePartitions[T](dataset: Dataset[T], partitionCol: String): Set[_ <: Any] = {
    dataset
      .select(partitionCol)
      .distinct()
      .filter(col(partitionCol) isNotNull)
      .rdd
      .map { case Row(p: Any) => p }
      .collect()
      .toSet
  }

  private def newWriter[T](ds: Dataset[T], mode: String = "overwrite"): DataFrameWriter[T] = {

    val session = ds.sparkSession
    val codecKey = "spark.sql.parquet.compression.codec"
    val codec = session.conf.get(codecKey, defaultCompressionCodec)
    val blockSizeKey = "parquet.block.size"
    val hc = session.sparkContext.hadoopConfiguration
    val blockSize = hc.get(blockSizeKey, defaultBlockSize.toString)

    ds.write
      .mode(mode)
      .format(getDatasourceFormat(ds.sparkSession))
      .option(codecKey, codec)
      .option(blockSizeKey, blockSize)
  }
}

object DirectoryHelper extends HLSLogging {

  /**
   * Checks to see if a directory was written to with the directory atomic commit protocol.
   *
   * Note that since this protocol is only available on actual Databricks clusters, this function
   * can't be fully tested in unit tests.
   */
  def directoryWasCommitted(session: SparkSession, path: String): Boolean = {
    val hPath = new Path(path)
    val fs = hPath.getFileSystem(session.sparkContext.hadoopConfiguration)

    // List files instead of using `exists` directly to avoid possible issues with S3 negative
    // caching.
    try {
      fs.listStatus(hPath).exists(_.getPath.getName == DIRECTORY_COMMIT_SUCCESS_FILE)
    } catch {
      case _: FileNotFoundException => false
    }
  }

  val DIRECTORY_COMMIT_SUCCESS_FILE = "_SUCCESS"
}
