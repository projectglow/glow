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

package io.projectglow.plink

import scala.collection.JavaConverters._
import com.google.common.io.LittleEndianDataInputStream
import com.univocity.parsers.csv.CsvParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType
import io.projectglow.common.{GlowLogging, VariantSchemas}
import io.projectglow.sql.util.SerializableConfiguration
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.execution.datasources.csv.{CSVOptions, CSVUtils, UnivocityParser, UnivocityParserUtils}
import org.apache.spark.sql.internal.SQLConf

class PlinkFileFormat
    extends FileFormat
    with DataSourceRegister
    with Serializable
    with GlowLogging {

  import PlinkFileFormat._

  override def shortName(): String = "plink"

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    Some(VariantSchemas.plinkSchema)
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException(
      "PLINK data source does not support writing sharded files."
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

    val sampleIds = PlinkFileFormat.getSampleIds(options, hadoopConf)
    val variants = PlinkFileFormat.getVariants(options, hadoopConf)
    val serializableConf = new SerializableConfiguration(hadoopConf)

    file => {
      val path = new Path(file.filePath)
      val hadoopFs = path.getFileSystem(serializableConf.value)

      val stream = hadoopFs.open(path)
      val littleEndianStream = new LittleEndianDataInputStream(stream)

      Option(TaskContext.get()).foreach { tc =>
        tc.addTaskCompletionListener[Unit] { _ =>
          stream.close()
        }
      }

      verifyBed(littleEndianStream)
      val numSamples = sampleIds.length
      val blockSize = getBlockSize(numSamples)
      val firstVariantIdx = getFirstVariantIdx(file, blockSize)
      val firstVariantStart = getFirstVariantStart(firstVariantIdx, blockSize)
      stream.seek(firstVariantStart)

      val numVariants = getNumVariants(file, firstVariantStart, blockSize)
      val relevantVariants =
        variants.slice(firstVariantIdx, firstVariantIdx + numVariants)

      val bedIter = new BedFileIterator(
        littleEndianStream,
        stream,
        numSamples,
        numVariants,
        blockSize
      )

      val rowConverter = new PlinkRowToInternalRowConverter(requiredSchema)
      relevantVariants.toIterator.zip(bedIter).map {
        case (variant, gtBlock) =>
          rowConverter.convertRow(variant, sampleIds, gtBlock)
      }
    }
  }
}

object PlinkFileFormat extends GlowLogging {

  import io.projectglow.common.VariantSchemas._

  val CSV_DELIMITER_KEY = "delimiter"
  val FAM_DELIMITER_KEY = "fam_delimiter"
  val BIM_DELIMITER_KEY = "bim_delimiter"
  val DEFAULT_DELIMITER_VALUE = " "

  val FAM_PATH_KEY = "fam"
  val BIM_PATH_KEY = "bim"

  val FAM_FILE_EXTENSION = ".fam"
  val BIM_FILE_EXTENSION = ".bim"

  val BLOCKS_PER_BYTE = 4
  val MAGIC_BYTES: Seq[Byte] = Seq(0x6c, 0x1b, 0x01).map(_.toByte)
  val NUM_MAGIC_BYTES: Int = MAGIC_BYTES.size

  def getPrefixPath(options: Map[String, String]): String = {
    val bedPath =
      options.getOrElse("path", throw new IllegalArgumentException("Must provide path."))
    bedPath.dropRight(4)
  }

  // Parses FAM file to get the sample IDs
  def getSampleIds(options: Map[String, String], hadoopConf: Configuration): Array[String] = {
    val famPathStr = options.get(FAM_PATH_KEY) match {
      case Some(s) => s
      case None => getPrefixPath(options) + FAM_FILE_EXTENSION
      // For now, the user must have a FAM path
    }
    logger.info(s"Using FAM file $famPathStr")

    val famPath = new Path(famPathStr)
    val hadoopFs = famPath.getFileSystem(hadoopConf)
    val stream = hadoopFs.open(famPath)
    val lines = IOUtils.lineIterator(stream, "UTF-8").asScala

    val famDelimiterOption = options.getOrElse(FAM_DELIMITER_KEY, DEFAULT_DELIMITER_VALUE)
    val parsedOptions =
      new CSVOptions(
        Map(CSV_DELIMITER_KEY -> famDelimiterOption),
        SQLConf.get.csvColumnPruning,
        SQLConf.get.sessionLocalTimeZone)

    val filteredLines = CSVUtils.filterCommentAndEmpty(lines, parsedOptions)
    val parser = new CsvParser(parsedOptions.asParserSettings)
    val sampleIdIterator = filteredLines.map { l =>
      val sampleLine = parser.parseRecord(l)
      require(
        sampleLine.getValues.length == 6,
        s"Failed while parsing FAM file $famPath: does not have 6 columns delimited by '$famDelimiterOption'")
      val familyId = sampleLine.getString(0)
      val individualId = sampleLine.getString(1)
      s"${familyId}_$individualId"
    }
    sampleIdIterator.toArray
  }

  // Parses BIM file to get the variants
  def getVariants(options: Map[String, String], hadoopConf: Configuration): Array[InternalRow] = {
    val bimPathStr = options.get(BIM_PATH_KEY) match {
      case Some(s) => s
      case None => getPrefixPath(options) + BIM_FILE_EXTENSION
      // For now, the user must have a BIM path
    }
    logger.info(s"Using BIM file $bimPathStr")

    val bimPath = new Path(bimPathStr)
    val hadoopFs = bimPath.getFileSystem(hadoopConf)
    val stream = hadoopFs.open(bimPath)
    val lines = IOUtils.lineIterator(stream, "UTF-8").asScala

    val bimDelimiterOption = options.getOrElse(BIM_DELIMITER_KEY, DEFAULT_DELIMITER_VALUE)
    val parsedOptions =
      new CSVOptions(
        Map(CSV_DELIMITER_KEY -> bimDelimiterOption, "mode" -> FailFastMode.name),
        SQLConf.get.csvColumnPruning,
        SQLConf.get.sessionLocalTimeZone)

    val filteredLines = CSVUtils.filterCommentAndEmpty(lines, parsedOptions)
    val univocityParser = new UnivocityParser(bimSchema, bimSchema, parsedOptions)
    try {
      val variantIterator =
        UnivocityParserUtils.parseIterator(filteredLines, univocityParser, bimSchema)
      variantIterator.map(_.copy).toArray
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Failed while parsing BIM file $bimPath: ${e.getMessage}")
    }
  }

  def getBlockSize(numSamples: Int): Int = {
    math.ceil(numSamples / BLOCKS_PER_BYTE.toDouble).toInt
  }

  def getFirstVariantIdx(partitionedFile: PartitionedFile, blockSize: Int): Int = {
    math.max(
      0,
      math.ceil((partitionedFile.start - (NUM_MAGIC_BYTES.toDouble - 1)) / blockSize).toInt)
  }

  def getFirstVariantStart(variantIdx: Int, blockSize: Int): Int = {
    NUM_MAGIC_BYTES + (blockSize * variantIdx)
  }

  def getNumVariants(
      partitionedFile: PartitionedFile,
      firstVariantStart: Int,
      blockSize: Int): Int = {
    math.ceil((partitionedFile.length - firstVariantStart) / blockSize.toDouble).toInt
  }

  def verifyBed(stream: LittleEndianDataInputStream): Unit = {
    val magicNumber = MAGIC_BYTES.map(_ => stream.readByte())

    lazy val hexString = MAGIC_BYTES.map { b =>
      String.format("%04x", Byte.box(b))
    }.mkString(",")
    require(
      magicNumber == MAGIC_BYTES,
      s"Magic bytes were not $hexString; this is not a variant-major BED."
    )
  }
}
