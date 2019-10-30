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
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.execution.datasources.csv.{CSVOptions, CSVUtils, UnivocityParser, UnivocityParserUtils}
import org.apache.spark.sql.internal.SQLConf

import io.projectglow.common.{GlowLogging, VariantSchemas}
import io.projectglow.common.logging.{HlsMetricDefinitions, HlsTagDefinitions, HlsTagValues, HlsUsageLogging}
import io.projectglow.sql.util.SerializableConfiguration

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

    val sampleIds = getSampleIds(options, hadoopConf) // Read sample IDs from a FAM file
    val variants = getVariants(options, hadoopConf) // Read variant metadata from a BIM file

    val serializableConf = new SerializableConfiguration(hadoopConf)
    logPlinkRead(options)

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
      val firstVariantIdx = getFirstVariantIdx(file.start, blockSize)
      val firstVariantStart = getVariantStart(firstVariantIdx, blockSize)
      val numVariants = getNumVariants(file.start, file.length, firstVariantStart, blockSize)

      // Read genotype calls from the BED file
      logger.info(s"Reading variants [$firstVariantIdx, ${firstVariantIdx + numVariants}]")
      stream.seek(firstVariantStart)
      val bedIter = new BedFileIterator(
        littleEndianStream,
        stream,
        numSamples,
        numVariants,
        blockSize
      )

      // Join the variant metadata, sample IDs and genotype calls
      val relevantVariants =
        variants.slice(firstVariantIdx, firstVariantIdx + numVariants)
      val rowConverter = new PlinkRowToInternalRowConverter(requiredSchema)
      relevantVariants.toIterator.zip(bedIter).map {
        case (variant, gtBlock) =>
          rowConverter.convertRow(variant, sampleIds, gtBlock)
      }
    }
  }
}

object PlinkFileFormat extends HlsUsageLogging {

  import io.projectglow.common.VariantSchemas._

  // Delimiter options
  val FAM_DELIMITER_KEY = "famDelimiter"
  val BIM_DELIMITER_KEY = "bimDelimiter"
  val DEFAULT_FAM_DELIMITER_VALUE = " "
  val DEFAULT_BIM_DELIMITER_VALUE = "\t"
  val CSV_DELIMITER_KEY = "delimiter"

  // Path options
  val FAM_PATH_KEY = "fam"
  val BIM_PATH_KEY = "bim"

  // Sample ID options
  val MERGE_FID_IID = "mergeFidIid"

  val FAM_FILE_EXTENSION = ".fam"
  val BIM_FILE_EXTENSION = ".bim"

  val BLOCKS_PER_BYTE = 4
  val MAGIC_BYTES: Seq[Byte] = Seq(0x6c, 0x1b, 0x01).map(_.toByte)
  val NUM_MAGIC_BYTES: Int = MAGIC_BYTES.size

  /* If a custom FAM or BIM path is not specified, use the BED's prefix as the default prefix  */
  def getPrefixPath(options: Map[String, String]): String = {
    val bedPath =
      options.getOrElse("path", throw new IllegalArgumentException("Must provide path."))
    bedPath.dropRight(4)
  }

  /* Parses a FAM file to get the sample IDs */
  def getSampleIds(options: Map[String, String], hadoopConf: Configuration): Array[String] = {
    val famPathStr = options.get(FAM_PATH_KEY) match {
      case Some(s) => s
      case None => getPrefixPath(options) + FAM_FILE_EXTENSION
    }
    logger.info(s"Using FAM file $famPathStr")

    val famDelimiterOption = options.getOrElse(FAM_DELIMITER_KEY, DEFAULT_FAM_DELIMITER_VALUE)
    val parsedOptions =
      new CSVOptions(
        Map(CSV_DELIMITER_KEY -> famDelimiterOption),
        SQLConf.get.csvColumnPruning,
        SQLConf.get.sessionLocalTimeZone)

    val mergeFidIid = try {
      options.getOrElse(MERGE_FID_IID, "true").toBoolean
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Value for $MERGE_FID_IID must be [true, false]. Provided: ${options(MERGE_FID_IID)}")
    }

    val famPath = new Path(famPathStr)
    val hadoopFs = famPath.getFileSystem(hadoopConf)
    val stream = hadoopFs.open(famPath)
    val lines = IOUtils.lineIterator(stream, "UTF-8").asScala
    val filteredLines = CSVUtils.filterCommentAndEmpty(lines, parsedOptions)
    val parser = new CsvParser(parsedOptions.asParserSettings)

    try {
      filteredLines.map { l =>
        val sampleLine = parser.parseRecord(l)
        require(
          sampleLine.getValues.length == 6,
          s"Failed while parsing FAM file $famPath: does not have 6 columns delimited by '$famDelimiterOption'")
        val individualId = sampleLine.getString(1)
        if (mergeFidIid) {
          val familyId = sampleLine.getString(0)
          s"${familyId}_$individualId"
        } else {
          individualId
        }
      }.toArray
    } finally {
      stream.close()
    }
  }

  /* Parses a BIM file to get the variants */
  def getVariants(options: Map[String, String], hadoopConf: Configuration): Array[InternalRow] = {
    val bimPathStr = options.get(BIM_PATH_KEY) match {
      case Some(s) => s
      case None => getPrefixPath(options) + BIM_FILE_EXTENSION
    }
    logger.info(s"Using BIM file $bimPathStr")

    val bimDelimiterOption = options.getOrElse(BIM_DELIMITER_KEY, DEFAULT_BIM_DELIMITER_VALUE)
    val parsedOptions =
      new CSVOptions(
        Map(CSV_DELIMITER_KEY -> bimDelimiterOption, "mode" -> FailFastMode.name),
        SQLConf.get.csvColumnPruning,
        SQLConf.get.sessionLocalTimeZone)

    val bimPath = new Path(bimPathStr)
    val hadoopFs = bimPath.getFileSystem(hadoopConf)
    val stream = hadoopFs.open(bimPath)
    val lines = IOUtils.lineIterator(stream, "UTF-8").asScala
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
    } finally {
      stream.close()
    }
  }

  /* Number of blocks used to represent a variant in the BED */
  def getBlockSize(numSamples: Int): Int = {
    math.ceil(numSamples / BLOCKS_PER_BYTE.toDouble).toInt
  }

  /* Index of the first variant to be parsed for a partitioned BED */
  def getFirstVariantIdx(partitionedFileStart: Long, blockSize: Int): Int = {
    math.max(0, math.ceil((partitionedFileStart - NUM_MAGIC_BYTES.toDouble) / blockSize).toInt)
  }

  /* The location of the first byte for a variant in a BED */
  def getVariantStart(variantIdx: Int, blockSize: Int): Int = {
    NUM_MAGIC_BYTES + (blockSize * variantIdx)
  }

  /* Number of variants to be read from a partitioned BED */
  def getNumVariants(
      partitionedFileStart: Long,
      partitionedFileLength: Long,
      firstVariantStart: Int,
      blockSize: Int): Int = {
    val actualLength = partitionedFileLength - (firstVariantStart - partitionedFileStart)
    math.ceil(actualLength / blockSize.toDouble).toInt
  }

  /* Check that a BED has the expected magic numbers (not true for sample-major BEDs or legacy files) */
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

  /* Log that PLINK files are being read, and whether custom FAM and BIM files were specified */
  def logPlinkRead(options: Map[String, String]): Unit = {
    val logOptions = Map(
      "customFam" -> options.contains(FAM_PATH_KEY),
      "customBim" -> options.contains(BIM_PATH_KEY)
    )
    recordHlsUsage(
      HlsMetricDefinitions.EVENT_HLS_USAGE,
      Map(
        HlsTagDefinitions.TAG_EVENT_TYPE -> HlsTagValues.EVENT_PLINK_READ
      ),
      blob = hlsJsonBuilder(logOptions)
    )
  }
}
