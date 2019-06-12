package com.databricks.sql

import java.io.ByteArrayOutputStream
import java.net.URI

import scala.reflect.ClassTag

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.io.IOUtils
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

private[databricks] sealed trait ParallelUploadResult {}

private[databricks] sealed trait ParallelUploader[T <: ParallelUploadResult] extends Serializable {

  def optMinPartSize: Option[Int] = None

  final def uploadPart(os: ByteArrayOutputStream, part: Int): T = {
    uploadPart(os.toByteArray, part)
  }

  def uploadPart(bytes: Array[Byte], part: Int): T

  def finalizeUpload(parts: Seq[T])

  final def finalizeUpload(parts: Array[ParallelUploadResult]) {
    finalizeUpload(parts.map {
      case parT: T =>
        parT
      case p =>
        throw new IllegalStateException("Received illegal return type %s from upload".format(p))
    }.toSeq)
  }
}

private[databricks] case class AmazonPartUploadResult(eTag: String, partId: Int)
    extends ParallelUploadResult {}

//private[databricks] object AmazonFileUploader {
//
//  def initiateUpload(path: URI, parts: Int): AmazonFileUploader = {
//
//    val s3 = new AmazonS3Client()
//
//    // quick and dirty parsing logic
//    val bucket = path.getAuthority
//    val key = path.getPath.drop(1) // begins with a /
//
//    val request = new InitiateMultipartUploadRequest(bucket, key)
//    val uploadId = s3.initiateMultipartUpload(request).getUploadId
//
//    AmazonFileUploader(bucket, key, uploadId)
//  }
//}
//
//private[databricks] case class AmazonFileUploader(bucket: String, key: String, uploadId: String)
//    extends ParallelUploader[AmazonPartUploadResult] {
//
//  override def optMinPartSize: Option[Int] = Some(5 * 1024 * 1024)
//
//  def uploadPart(bytes: Array[Byte], part: Int): AmazonPartUploadResult = {
//
//    val s3 = new AmazonS3Client()
//    val bais = new ByteArrayInputStream(bytes)
//    val response = s3.uploadPart(
//      new UploadPartRequest()
//        .withBucketName(bucket)
//        .withKey(key)
//        .withUploadId(uploadId)
//        .withPartNumber(part + 1)
//        .withInputStream(bais)
//        .withPartSize(bytes.length)
//    )
//
//    val eTag = response.getPartETag
//
//    AmazonPartUploadResult(eTag.getETag, part)
//  }
//
//  def finalizeUpload(parts: Seq[AmazonPartUploadResult]) {
//
//    val eTags = parts.map(p => {
//      new PartETag(p.partId + 1, p.eTag)
//    })
//
//    val s3 = new AmazonS3Client()
//
//    s3.completeMultipartUpload(
//      new CompleteMultipartUploadRequest()
//        .withBucketName(bucket)
//        .withKey(key)
//        .withUploadId(uploadId)
//        .withPartETags(eTags.toList.asJava)
//    )
//  }
//}

private[databricks] object SingleFileWriter extends HLSLogging {

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

//    // TODO: support DBFS, Azure, s3 without IAM role configured
//    if (uri.getScheme == "s3" ||
//      uri.getScheme == "s3a") {
//
//      val uploader = AmazonFileUploader.initiateUpload(uri, rdd.partitions.length)
//      writeFileParallel[AmazonPartUploadResult](rdd, uploader)
//    } else {
      logger.info(s"Writing file from driver")
      writeFileFromDriver(new Path(uri), rdd)
//    }
  }

  /**
   * Given an RDD of byte arrays that should be written out in a parallel file upload, repartition
   * it so that each partition except for the last is at least as large as the minimum partition
   * size. Both the input and output RDDs should contain a single byte array per partition.
   * @param rdd
   * @param minPartitionSizeB
   * @return
   */
  @VisibleForTesting
  private[sql] def repartitionRddToUpload(
      rdd: RDD[Array[Byte]],
      minPartitionSizeB: Int): RDD[Array[Byte]] = {

    val partitionByteSizes = rdd.map(_.length.toLong).collect
    val totalBytes = partitionByteSizes.sum
    val smallestPartition = partitionByteSizes.min

    if (smallestPartition > minPartitionSizeB || partitionByteSizes.length == 1) {
      // RDD is fine to upload
      rdd
    } else {
      val desiredPartitionSize = Math.min(minPartitionSizeB * 2, totalBytes)
      val partitionStartsIdxs = partitionByteSizes.dropRight(1).scan(0L)(_ + _)

      val shuffle = rdd.mapPartitionsWithIndex {
        case (idx, iter) =>
          val bytes = iter.next()
          require(!iter.hasNext, "Input RDD should have one byte array per partition")

          val partStart = partitionStartsIdxs(idx)
          val partEnd = partStart + bytes.length
          val destPartStart = partStart / desiredPartitionSize
          val destPartEnd = (partEnd - 1) / desiredPartitionSize

          (destPartStart to destPartEnd).map { part =>
            val startIndex = Math.max(0, part * desiredPartitionSize - partStart)
            val endIndex = Math.min(bytes.length, (part + 1) * desiredPartitionSize - partStart)

            // Emit the current partition as well as the desired partition so that the records
            // in each partition are sorted correctly
            ((idx, part.toInt), bytes.slice(startIndex.toInt, endIndex.toInt))
          }.toIterator
      }

      shuffle
        .repartitionAndSortWithinPartitions(
          new ManualRegionPartitioner(Math.ceil(totalBytes / desiredPartitionSize.toDouble).toInt)
        )
        .mapPartitions(iter => Iterator(iter.map(_._2).reduce(_ ++ _)))
    }
  }

  private def writeFileParallel[U <: ParallelUploadResult](
      byteRdd: RDD[Array[Byte]],
      uploader: ParallelUploader[U])(implicit uTag: ClassTag[U]) {

    // how many parts will we be uploading
    val parts = byteRdd.partitions.length
    require(
      parts < 10000,
      ("Cannot do a multipart upload with more than 10000 parts. " +
      "RDD has (%d) partitions and a header.").format(parts)
    )

    // write each partition out, and upload them in parallel
    val rddToUpload = uploader.optMinPartSize match {
      case None => byteRdd
      case Some(minSize) => repartitionRddToUpload(byteRdd, minSize)
    }

    val partitionUploads = rddToUpload
      .mapPartitionsWithIndex {
        case (idx, iter) =>
          // upload the part and return any metadata
          Iterator((idx, uploader.uploadPart(iter.next(), idx)))
      }
      .collect()

    // add the header metadata if it exists and sort the upload metadata
    val sortedMetadata = partitionUploads
      .sortBy(_._1)
      .map(_._2)

    uploader.finalizeUpload(sortedMetadata)
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
