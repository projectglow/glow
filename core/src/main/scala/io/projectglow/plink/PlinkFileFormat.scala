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
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.catalyst.expressions.{Add, BoundReference, CreateArray, GenericInternalRow, Length, Literal, MutableProjection, Subtract}
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.execution.datasources.csv.CSVUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import io.projectglow.common.{CommonOptions, GlowLogging, VariantSchemas}
import io.projectglow.common.logging.{HlsEventRecorder, HlsTagValues}
import io.projectglow.sql.util.SerializableConfiguration

import java.net.URI

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
    val includeSampleIds =
      options.get(CommonOptions.INCLUDE_SAMPLE_IDS).forall(_.toBoolean)
    Some(VariantSchemas.plinkSchema(includeSampleIds))
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException(
      "PLINK data source does not support writing."
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
      hadoopConf: Configuration
  ): PartitionedFile => Iterator[InternalRow] = {

    val serializableConf = new SerializableConfiguration(hadoopConf)
    logPlinkRead(options)

    file => {
      val path = file.filePath.toPath
      val hadoopFs = path.getFileSystem(serializableConf.value)
      val stream = hadoopFs.open(path)
      val littleEndianStream = new LittleEndianDataInputStream(stream)

      Option(TaskContext.get()).foreach { tc =>
        tc.addTaskCompletionListener[Unit] { _ =>
          stream.close()
        }
      }

      // Read sample IDs from the accompanying FAM file
      val sampleIds =
        getSampleIds(file.filePath.toString, options, serializableConf.value)

      verifyBed(littleEndianStream)
      val numSamples = sampleIds.length
      val blockSize = getBlockSize(numSamples)
      val firstVariantIdx = getFirstVariantIdx(file.start, blockSize)
      val firstVariantStart = getVariantStart(firstVariantIdx, blockSize)
      val numVariants =
        getNumVariants(file.start, file.length, firstVariantStart, blockSize)

      // Read the relevant variant metadata from the accompanying BIM file
      val variants =
        getVariants(
          file.filePath.toString,
          firstVariantIdx,
          numVariants,
          options,
          serializableConf.value
        )

      // Read genotype calls from the BED file
      logger.info(
        s"Reading variants [$firstVariantIdx, ${firstVariantIdx + numVariants}]"
      )
      stream.seek(firstVariantStart)
      val bedIter = new BedFileIterator(
        littleEndianStream,
        stream,
        numVariants,
        blockSize
      )

      // Join the variant metadata, sample IDs and genotype calls
      val projection = makeMutableProjection(requiredSchema)
      val rowConverter = new PlinkRowToInternalRowConverter(requiredSchema)
      variants.toIterator.zip(bedIter).map { case (variant, gtBlock) =>
        rowConverter.convertRow(projection(variant), sampleIds, gtBlock)
      }
    }
  }
}

object PlinkFileFormat extends HlsEventRecorder {

  import io.projectglow.common.VariantSchemas._
  import io.projectglow.common.PlinkOptions._

  val FAM_FILE_EXTENSION = ".fam"
  val BIM_FILE_EXTENSION = ".bim"
  val CSV_DELIMITER_KEY = "delimiter"

  val BLOCKS_PER_BYTE = 4
  val MAGIC_BYTES: Seq[Byte] = Seq(0x6c, 0x1b, 0x01).map(_.toByte)
  val NUM_MAGIC_BYTES: Int = MAGIC_BYTES.size

  /* Log that PLINK files are being read */
  def logPlinkRead(options: Map[String, String]): Unit = {
    val logOptions = Map(
      CommonOptions.INCLUDE_SAMPLE_IDS -> options
        .get(CommonOptions.INCLUDE_SAMPLE_IDS)
        .forall(_.toBoolean),
      MERGE_FID_IID -> options
        .get(CommonOptions.INCLUDE_SAMPLE_IDS)
        .forall(_.toBoolean)
    )
    recordHlsEvent(HlsTagValues.EVENT_PLINK_READ, logOptions)
  }

  /* Gets the BED path's prefix as the FAM and BIM path prefix  */
  def getPrefixPath(bedPath: String): String = {
    bedPath.dropRight(4)
  }

  /* Parses a FAM file to get the sample IDs */
  def getSampleIds(
      bedPath: String,
      options: Map[String, String],
      hadoopConf: Configuration): Array[UTF8String] = {
    val famDelimiterOption = {
      options.getOrElse(FAM_DELIMITER_KEY, DEFAULT_FAM_DELIMITER_VALUE)
    }
    val format = new CsvFormat()
    format.setDelimiter(famDelimiterOption)
    val settings = new CsvParserSettings()
    settings.setSkipEmptyLines(true)
    settings.setFormat(format)

    val mergeFidIid =
      try {
        options.getOrElse(MERGE_FID_IID, "true").toBoolean
      } catch {
        case _: IllegalArgumentException =>
          throw new IllegalArgumentException(
            s"Value for $MERGE_FID_IID must be [true, false]. Provided: ${options(MERGE_FID_IID)}"
          )
      }

    val prefixPath = getPrefixPath(bedPath)
    val famPath = new Path(new URI(prefixPath + FAM_FILE_EXTENSION))
    val hadoopFs = famPath.getFileSystem(hadoopConf)
    val stream = hadoopFs.open(famPath)
    val parser = new CsvParser(settings)

    try {
      val filteredLines = parser.parseAll(stream).asScala
      filteredLines.map { sampleLine =>
        require(
          sampleLine.length == 6,
          s"Failed while parsing FAM file $famPath: does not have 6 columns delimited by '$famDelimiterOption'"
        )
        val individualId = sampleLine(1)
        val sampleId = if (mergeFidIid) {
          val familyId = sampleLine(0)
          s"${familyId}_$individualId"
        } else {
          individualId
        }
        UTF8String.fromString(sampleId)
      }.toArray
    } finally {
      stream.close()
    }
  }

  /* Parses a BIM file to get the relevant variants. The schema of the returned rows has the following fields:
       - chromosome (string)
       - variant ID (string)
       - position (double)
       - base-pair coordinate, 1-based (int)
       - allele 1, assumed to be alternate (string)
       - allele 2, assumed to be reference (string)
   */
  def getVariants(
      bedPath: String,
      firstVariant: Int,
      numVariants: Int,
      options: Map[String, String],
      hadoopConf: Configuration): Array[InternalRow] = {
    val bimDelimiterOption =
      options.getOrElse(BIM_DELIMITER_KEY, DEFAULT_BIM_DELIMITER_VALUE)
    val format = new CsvFormat()
    format.setDelimiter(bimDelimiterOption)
    val settings = new CsvParserSettings()
    settings.setSkipEmptyLines(true)
    settings.setFormat(format)
    val parser = new CsvParser(settings)
    val prefixPath = getPrefixPath(bedPath)
    val bimPath = new Path(new URI(prefixPath + BIM_FILE_EXTENSION))
    val hadoopFs = bimPath.getFileSystem(hadoopConf)
    val stream = hadoopFs.open(bimPath)

    try {
      val variantLines =
        parser.parseAll(stream).asScala.slice(firstVariant, firstVariant + numVariants)
      variantLines.map { l =>
        val row = new GenericInternalRow(bimSchema.length)
        row.update(0, UTF8String.fromString(l(0)))
        row.update(1, UTF8String.fromString(l(1)))
        row.setDouble(2, l(2).toDouble)
        row.setLong(3, l(3).toLong)
        row.update(4, UTF8String.fromString(l(4)))
        row.update(5, UTF8String.fromString(l(5)))
        row
      }.toArray
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Failed while parsing BIM file $bimPath: ${e.getMessage}"
        )
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
    math.max(
      0,
      math
        .ceil((partitionedFileStart - NUM_MAGIC_BYTES.toDouble) / blockSize)
        .toInt
    )
  }

  /* The location of the first byte for a variant in a BED */
  def getVariantStart(variantIdx: Int, blockSize: Int): Long = {
    NUM_MAGIC_BYTES + (blockSize * variantIdx.toLong)
  }

  /* Number of variants to be read from a partitioned BED */
  def getNumVariants(
      partitionedFileStart: Long,
      partitionedFileLength: Long,
      firstVariantStart: Long,
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

  /* Project BIM rows to the output schema */
  def makeMutableProjection(schema: StructType): MutableProjection = {
    val expressions =
      schema.map {
        case `contigNameField` => makeBimBoundReference(contigNameField)
        case `namesField` =>
          CreateArray(Seq(makeBimBoundReference(variantIdField)))
        case `positionField` => makeBimBoundReference(positionField)
        case `startField` =>
          Subtract(makeBimBoundReference(startField), Literal(1)) // BIM is 1-start
        case `endField` =>
          Add(
            Subtract(makeBimBoundReference(startField), Literal(1)),
            Length(makeBimBoundReference(alleleTwoField))
          )
        case `refAlleleField` => makeBimBoundReference(alleleTwoField)
        case `alternateAllelesField` =>
          CreateArray(Seq(makeBimBoundReference(alleleOneField)))
        case f => Literal(null, f.dataType)
      }
    GenerateMutableProjection.generate(expressions)
  }

  def makeBimBoundReference(f: StructField): BoundReference = {
    val idx = bimSchema.indexOf(f)
    BoundReference(idx, f.dataType, f.nullable)
  }
}
