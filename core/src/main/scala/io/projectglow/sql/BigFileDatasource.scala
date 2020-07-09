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

package io.projectglow.sql

import java.net.URI
import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import io.projectglow.common.{GlowLogging, WithUtils}

/**
 * Base class for big file datasources. Handles plumbing that's necessary for all such sources:
 * - Checking the save mode
 * - Uploading an RDD of byte arrays
 */
abstract class BigFileDatasource extends CreatableRelationProvider {

  /**
   * Implemented by subclasses. Must return an RDD where each partition is exactly 1 byte array.
   */
  protected def serializeDataFrame(options: Map[String, String], df: DataFrame): RDD[Array[Byte]]

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      options: Map[String, String],
      data: DataFrame): BaseRelation = {

    val path = BigFileDatasource.checkPath(options)
    val filesystemPath = new Path(path)
    val hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConfWithOptions(options)
    val fs = filesystemPath.getFileSystem(hadoopConf)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"Path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }

    if (doSave) {
      WithUtils.withCachedDataset(data) { cachedDs =>
        val byteRdd = serializeDataFrame(options, cachedDs)
        SingleFileWriter.write(byteRdd, path, hadoopConf)
      }
    }
    SingleFileRelation(sqlContext, data.schema)
  }
}

object BigFileDatasource {
  def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified"))
  }
}

case class SingleFileRelation(sqlContext: SQLContext, schema: StructType) extends BaseRelation

trait BigFileUploader {
  def canUpload(path: String, conf: Configuration): Boolean
  def upload(bytes: RDD[Array[Byte]], path: String, conf: Configuration): Unit
}

object SingleFileWriter extends GlowLogging {

  lazy val uploaders: Seq[BigFileUploader] = ServiceLoader
    .load(classOf[BigFileUploader])
    .iterator()
    .asScala
    .toSeq

  /**
   * Writes a single file in parallel to a storage system.
   *
   * Infers the destination storage system from the provided path.
   *
   * @param rdd  The RDD to write.
   * @param path The path to write the RDD to.
   */
  def write(rdd: RDD[Array[Byte]], path: String, hadoopConf: Configuration) {
    val uri = new URI(path)
    uploaders.find(_.canUpload(path, hadoopConf)) match {
      case Some(uploader) => uploader.upload(rdd, path, hadoopConf)
      case None =>
        logger.info(s"Could not find a parallel uploader for $path, uploading from the driver")
        writeFileFromDriver(new Path(uri), rdd, hadoopConf)
    }
  }

  private def writeFileFromDriver(
      path: Path,
      byteRdd: RDD[Array[Byte]],
      hadoopConf: Configuration): Unit = {
    val fs = path.getFileSystem(hadoopConf)
    WithUtils.withCloseable(fs.create(path)) { stream =>
      WithUtils.withCachedRDD(byteRdd) { cachedRdd =>
        cachedRdd.count()
        cachedRdd.toLocalIterator.foreach { chunk =>
          stream.write(chunk)
        }
      }
    }
  }
}
