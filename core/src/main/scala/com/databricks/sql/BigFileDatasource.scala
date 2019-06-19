package com.databricks.sql

import java.io.ByteArrayOutputStream
import java.net.URI
import java.util.ServiceLoader

import scala.collection.JavaConverters._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import com.databricks.hls.common.HLSLogging

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
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore => false
      }
    } else {
      true
    }

    if (doSave) {
      val byteRdd = serializeDataFrame(options, data)
      SingleFileWriter.write(byteRdd, path)
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
  def canUpload(conf: Configuration, path: String): Boolean
  def upload(bytes: RDD[Array[Byte]], path: String): Unit
}

private[databricks] object SingleFileWriter extends HLSLogging {

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
   * @param rdd The RDD to write.
   * @param path The path to write the RDD to.
   */
  def write(rdd: RDD[Array[Byte]], path: String) {
    val uri = new URI(path)
    uploaders.find(_.canUpload(rdd.sparkContext.hadoopConfiguration, path)) match {
      case Some(uploader) => uploader.upload(rdd, path)
      case None =>
        logger.info(s"Could not find a parallel uploader for $path, uploading from the driver")
        writeFileFromDriver(new Path(uri), rdd)
    }
  }

  private def writeFileFromDriver(path: Path, byteRdd: RDD[Array[Byte]]): Unit = {
    val sc = byteRdd.sparkContext
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    val stream = fs.create(path)
    try {
      byteRdd.cache()
      byteRdd.count()
      byteRdd.toLocalIterator.foreach { chunk =>
        stream.write(chunk)
      }
    } finally {
      byteRdd.unpersist()
      IOUtils.closeQuietly(stream)
    }
  }
}
