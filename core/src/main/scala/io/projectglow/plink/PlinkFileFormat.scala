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

    val bedPath =
      options.getOrElse("path", throw new IllegalArgumentException("Must provide path."))
    val prefixPath = bedPath.dropRight(4)
    val sampleIds = PlinkFileFormat.getSampleIds(prefixPath, options, hadoopConf)
    val variants = PlinkFileFormat.getVariants(prefixPath, options, hadoopConf)

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

      val bedIter = new BedFileIterator(
        littleEndianStream,
        stream,
        numSamples,
        blockSize,
        file.start + file.length
      )

      val numVariantsInBlock = getNumVariants(file, firstVariantStart, blockSize)
      val relevantVariants =
        variants.slice(firstVariantStart, firstVariantStart + numVariantsInBlock)

      val rowConverter = new PlinkRowToInternalRowConverter(requiredSchema)
      relevantVariants.toIterator.zip(bedIter).map {
        case (variant, gtBlock) =>
          rowConverter.convertRow(variant, sampleIds, gtBlock)
      }
    }
  }
}

object PlinkFileFormat {

  import io.projectglow.common.VariantSchemas._

  val CSV_DELIMITER_KEY = "delimiter"
  val FAM_DELIMITER_KEY = "fam_delimiter"
  val BIM_DELIMITER_KEY = "bim_delimiter"
  val DEFAULT_DELIMITER_VALUE = " "

  val BLOCKS_PER_BYTE = 4
  val MAGIC_BYTES: Seq[Byte] = Seq(0x6c, 0x1b, 0x01).map(_.toByte)
  val NUM_MAGIC_BYTES: Int = MAGIC_BYTES.size

  // Parses FAM file to get the sample IDs
  def getSampleIds(
      prefixPath: String,
      options: Map[String, String],
      hadoopConf: Configuration): Array[String] = {
    val famDelimiterOption = options.getOrElse(FAM_DELIMITER_KEY, DEFAULT_DELIMITER_VALUE)
    val parsedOptions =
      new CSVOptions(
        Map(CSV_DELIMITER_KEY -> famDelimiterOption),
        SQLConf.get.csvColumnPruning,
        SQLConf.get.sessionLocalTimeZone)
    val famPath = new Path(prefixPath + ".fam")
    val hadoopFs = famPath.getFileSystem(hadoopConf)
    val stream = hadoopFs.open(famPath)
    val lines = IOUtils.lineIterator(stream, "UTF-8").asScala
    val filteredLines = CSVUtils.filterCommentAndEmpty(lines, parsedOptions)
    val parser = new CsvParser(parsedOptions.asParserSettings)
    val sampleIdIterator = filteredLines.map { l =>
      val sampleLine = parser.parseRecord(l)
      sampleLine.getString(0)
    }
    sampleIdIterator.toArray
  }

  // Parses BIM file to get the variants
  def getVariants(
      prefixPath: String,
      options: Map[String, String],
      hadoopConf: Configuration): Array[InternalRow] = {
    val bimDelimiterOption = options.getOrElse(BIM_DELIMITER_KEY, DEFAULT_DELIMITER_VALUE)
    val parsedOptions =
      new CSVOptions(
        Map(CSV_DELIMITER_KEY -> bimDelimiterOption),
        SQLConf.get.csvColumnPruning,
        SQLConf.get.sessionLocalTimeZone)
    val bimPath = new Path(prefixPath + ".bim")
    val hadoopFs = bimPath.getFileSystem(hadoopConf)
    val stream = hadoopFs.open(bimPath)
    val lines = IOUtils.lineIterator(stream, "UTF-8").asScala
    val filteredLines = CSVUtils.filterCommentAndEmpty(lines, parsedOptions)
    val univocityParser = new UnivocityParser(bimSchema, bimSchema, parsedOptions)
    val variantIterator =
      UnivocityParserUtils.parseIterator(filteredLines, univocityParser, bimSchema)
    variantIterator.toArray
  }

  def getBlockSize(numSamples: Int): Int = {
    math.ceil(numSamples / BLOCKS_PER_BYTE.toDouble).toInt
  }

  def getFirstVariantIdx(partitionedFile: PartitionedFile, blockSize: Int): Int = {
    math.ceil((partitionedFile.start - NUM_MAGIC_BYTES.toDouble) / blockSize).toInt
  }

  def getFirstVariantStart(variantIdx: Int, blockSize: Int): Int = {
    NUM_MAGIC_BYTES + blockSize * variantIdx
  }

  def getNumVariants(
      partitionedFile: PartitionedFile,
      firstVariantStart: Int,
      blockSize: Int): Int = {
    math.floor((partitionedFile.length - firstVariantStart) / blockSize.toDouble).toInt
  }

  def verifyBed(stream: LittleEndianDataInputStream): Unit = {
    val magicNumber = MAGIC_BYTES.map(_ => stream.readByte())

    require(
      magicNumber == MAGIC_BYTES,
      s"Magic bytes were not '0x6c', '0x1b', '0x01'; this is not a variant-major BED."
    )
  }
}
