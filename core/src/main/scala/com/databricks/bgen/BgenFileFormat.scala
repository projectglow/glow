package com.databricks.bgen

import java.io.{BufferedReader, File, InputStreamReader}
import java.nio.file.Paths

import scala.collection.JavaConverters._

import com.google.common.io.LittleEndianDataInputStream
import com.google.common.util.concurrent.Striped
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import org.skife.jdbi.v2.DBI
import org.skife.jdbi.v2.util.LongMapper

import com.databricks.hls.common.{HLSLogging, WithUtils}
import com.databricks.hls.sql.util.SerializableConfiguration
import com.databricks.vcf.VariantSchemas

class BgenFileFormat extends FileFormat with DataSourceRegister with Serializable with HLSLogging {

  val SAMPLE_FILE_PATH_OPTION_KEY = "sampleFilePath"
  val SAMPLE_ID_COLUMN_OPTION_KEY = "sampleIdColumn"
  val SAMPLE_ID_COLUMN_OPTION_DEFAULT_VALUE = "ID_2"

  override def shortName(): String = "com.databricks.bgen"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    Some(VariantSchemas.bgenDefaultSchema)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException(
      "BGEN data source does not support writing sharded BGENs; use com.databricks.bigbgen."
    )
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    true
  }

  override def buildReader(
      spark: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val serializableConf = new SerializableConfiguration(hadoopConf)
    val useIndex = options.get(BgenFileFormat.USE_INDEX_KEY).forall(_.toBoolean)
    val ignoreExtension = options.get(BgenFileFormat.IGNORE_EXTENSION_KEY).exists(_.toBoolean)
    val sampleIdsOpt = getSampleIds(options, hadoopConf)

    file => {
      val path = new Path(file.filePath)
      val hadoopFs = path.getFileSystem(serializableConf.value)
      nextVariantIndex(hadoopFs, file, useIndex, ignoreExtension) match {
        case None =>
          Iterator.empty
        case Some(pos) =>
          logger.info(s"Next variant index is $pos")
          val stream = hadoopFs.open(path)
          val littleEndianStream = new LittleEndianDataInputStream(stream)

          Option(TaskContext.get()).foreach { tc =>
            tc.addTaskCompletionListener[Unit] { _ =>
              stream.close()
            }
          }

          val header = new BgenHeaderReader(littleEndianStream).readHeader(sampleIdsOpt)
          val startPos = Math.max(pos, header.firstVariantOffset)
          stream.seek(startPos)
          val rowConverter = new BgenRowToInternalRowConverter(requiredSchema)

          val iter = new BgenFileIterator(
            header,
            littleEndianStream,
            stream,
            file.start,
            file.start + file.length
          )

          iter.init()
          iter.map(rowConverter.convertRow)
      }
    }
  }

  /**
   * Returns the position where the iterator for a given file split should start looking for
   * variants, or None no variants start in this block.
   *
   * Note that the file iterator should not necessarily begin returning variants from the index
   * returned by this file -- it may need to skip over part of the file until it reaches
   * the allotted file split.
   */
  private def nextVariantIndex(
      hadoopFs: FileSystem,
      file: PartitionedFile,
      useIndex: Boolean,
      ignoreExtension: Boolean): Option[Long] = {

    if (!file.filePath.endsWith(".bgen") && !ignoreExtension) {
      return None
    }

    val indexFile = new Path(file.filePath + BgenFileFormat.INDEX_SUFFIX)
    if (hadoopFs.exists(indexFile) && useIndex) {
      logger.info(s"Found index file ${indexFile} for BGEN file ${file.filePath}")
      val localIdxPath = downloadIdxIfNecessary(hadoopFs, indexFile)
      val dbi = new DBI(s"jdbc:sqlite:$localIdxPath")
      WithUtils.withCloseable(dbi.open()) { handle =>
        val query = handle
          .createQuery(BgenFileFormat.NEXT_IDX_QUERY)
          .bind("pos", file.start)
          .map(LongMapper.FIRST)

        Option(query.first())
      }
    } else {
      Some(0) // have to start at beginning of file :(
    }
  }

  private def downloadIdxIfNecessary(hadoopFs: FileSystem, path: Path): String = {
    val localDir = Paths.get(System.getProperty("java.io.tmpdir")).resolve("bgen_indices").toFile
    localDir.mkdirs()
    val localPath = s"$localDir/${path.getName.replaceAllLiterally("/", "__")}"
    WithUtils.withLock(BgenFileFormat.idxLock.get(path)) {
      if (!new File(localPath).exists()) {
        hadoopFs.copyToLocalFile(path, new Path("file:" + localPath))
      }
    }

    localPath
  }

  /**
   * Given a path to an Oxford-style .sample file exists (option: sampleFilePath), reads the sample
   * IDs from the appropriate column (option: sampleIdColumn, default: ID_2).
   *
   * If the column does not exist in the file, or if at least one row is malformed (does not contain
   * a sample ID), an IllegalArgumentException is thrown.
   * If no path is given, None is returned; if a valid path is given, Some list is returned.
   */
  private def getSampleIds(
      options: Map[String, String],
      hadoopConf: Configuration): Option[Seq[String]] = {

    val samplePathOpt = options.get(SAMPLE_FILE_PATH_OPTION_KEY)

    if (samplePathOpt.isEmpty) {
      // No sample file path provided
      return None
    }

    val samplePath = samplePathOpt.get
    val path = new Path(samplePath)
    val hadoopFs = path.getFileSystem(hadoopConf)
    val stream = hadoopFs.open(path)
    val streamReader = new InputStreamReader(stream)
    val bufferedReader = new BufferedReader(streamReader)

    // The first two (2) lines in a .sample file are header lines
    val sampleCol =
      options.getOrElse(SAMPLE_ID_COLUMN_OPTION_KEY, SAMPLE_ID_COLUMN_OPTION_DEFAULT_VALUE)
    val sampleColIdx = bufferedReader.readLine().split(" ").indexOf(sampleCol)
    bufferedReader.readLine() // Column-type line

    WithUtils.withCloseable(stream) { s =>
      if (sampleColIdx == -1) {
        throw new IllegalArgumentException(s"No column named $sampleCol in $samplePath.")
      }
      val sampleLines = bufferedReader
        .lines()
        .iterator()
        .asScala
        .toList

      val sampleIds = sampleLines.map { line =>
        if (line.split(" ").length > sampleColIdx) {
          line.split(" ").apply(sampleColIdx)
        } else {
          throw new IllegalArgumentException(
            s"Malformed line in $samplePath: fewer than ${sampleColIdx + 1} columns in $line"
          )
        }
      }

      Some(sampleIds)
    }
  }
}

object BgenFileFormat {
  val INDEX_SUFFIX = ".bgi"
  val NEXT_IDX_QUERY =
    """
      |SELECT MIN(file_start_position) from Variant
      |WHERE file_start_position > :pos
    """.stripMargin
  val idxLock = Striped.lock(100)
  val IGNORE_EXTENSION_KEY = "ignoreExtension"
  val USE_INDEX_KEY = "useBgenIndex"
}
