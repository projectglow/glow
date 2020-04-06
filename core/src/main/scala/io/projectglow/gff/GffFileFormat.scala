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

package io.projectglow.gff

import io.projectglow.common.FeatureSchemas._
import io.projectglow.common.logging.HlsEventRecorder
import io.projectglow.common.{CompressionUtils, GlowLogging}
import io.projectglow.gff.GffFileFormat._
import io.projectglow.sql.util.{HadoopLineIterator, SerializableConfiguration}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.csv.{CSVFileFormat, CSVOptions, UnivocityParser}
import org.apache.spark.sql.execution.datasources.{DataSource, OutputWriterFactory, PartitionedFile, TextBasedFileFormat}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class GffFileFormat
    extends TextBasedFileFormat
    with DataSourceRegister
    with HlsEventRecorder
    with GlowLogging {

  private var codecFactory: CompressionCodecFactory = _

  override def shortName(): String = "gff"

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    if (codecFactory == null) {
      codecFactory = new CompressionCodecFactory(
        CompressionUtils
          .hadoopConfWithBGZ(sparkSession.sessionState.newHadoopConf())
      )
    }

    // Note: we check if a file is gzipped vs block gzipped during reading, so this will be true
    // for .gz files even if they aren't actually splittable
    codecFactory.getCodec(path).isInstanceOf[SplittableCompressionCodec]
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {

    val optionsWithDelimiter = options + ("sep" -> COLUMN_DELIMITER)

    val parsedOptions = new CSVOptions(
      optionsWithDelimiter,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone
    )

    val csv = GffFileFormat
      .createBaseDataset(sparkSession, files, parsedOptions)
      .select(attributesField.name)
      .filter(col(attributesField.name).isNotNull)

    // Use of ParsedAttributesToken helps update sep as soon as detected and avoid looking into
    // attributes array again to detect the separator when it is detected once.
    val attributesTokenZero = ParsedAttributesToken(None, Set[String]())

    val attributesToken =
      csv
        .queryExecution
        .toRdd
        .aggregate(attributesTokenZero)(
          { (t, r) =>
            val attrStr = r.getString(0)
            updateAttributesToken(t, attrStr)
          }, { (t1, t2) =>
            ParsedAttributesToken(
              t1.sep.orElse(t2.sep.orElse(None)),
              t1.tags ++ t2.tags
            )
          }
        )

    // Fields in gffBaseSchema first, then official attributes fields, then unofficial fields
    val officialAttributeFields =
      gffOfficialAttributeFields.foldLeft(Seq[StructField]()) { (s, f) =>
        if (attributesToken
            .tags
            .map(_.toLowerCase)
            .map(_.replaceAll("_", ""))
            .contains(f.name.toLowerCase)) {
          s :+ f
        } else {
          s
        }
      }

    val remainingTags = attributesToken.tags.filter(
        t => !officialAttributeFields
          .map(_.name.toLowerCase)
          .contains(t.replaceAll("_", "").toLowerCase)
      )

    val unofficialAttributeFields = remainingTags.foldLeft(Seq[StructField]()) { (s, t) =>
      s :+ StructField(t, StringType)
    }

    Option(
      StructType(
        gffBaseSchema.fields ++ officialAttributeFields ++ unofficialAttributeFields
      )
    )

  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException(
      "GFF data source does not currently support writing."
    )
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

    // TODO: record gffRead event in the log along with the options

    val serializableConf = new SerializableConfiguration(
      CompressionUtils.hadoopConfWithBGZ(hadoopConf)
    )

    val optionsWithDelimiter = options + ("sep" -> COLUMN_DELIMITER)

    val parsedOptions = new CSVOptions(
      optionsWithDelimiter,
      spark.sessionState.conf.csvColumnPruning,
      spark.sessionState.conf.sessionLocalTimeZone
    )

    partitionedFile => {
      val path = new Path(partitionedFile.filePath)
      val hadoopFs = path.getFileSystem(serializableConf.value)

      val isGzip =
        CompressionUtils.isGzip(partitionedFile, serializableConf.value)

      val offset: Option[(Long, Long)] =
        if (isGzip && partitionedFile.start == 0) {
          // Reading gzip file from beginning to end
          val fileLength = hadoopFs.getFileStatus(path).getLen
          Some((0, fileLength))
        } else if (isGzip) {
          // Skipping gzip file because task starts in the middle of the file
          None
        } else {
          Some(
            (
              partitionedFile.start,
              partitionedFile.start + partitionedFile.length
            )
          )
        }

      offset match {
        case None =>
          Iterator.empty

        case Some((startPos, endPos)) =>
          // need to read all as strings then convert to correct types as some numeric
          // columns can have string values such as "."
          val parser =
            new UnivocityParser(gffCsvSchema, gffCsvSchema, parsedOptions)

          val lines = {
            val linesReader = new HadoopLineIterator(
              partitionedFile.filePath,
              startPos,
              endPos - startPos,
              None,
              serializableConf.value
            )

            Option(TaskContext.get()).foreach(
              _.addTaskCompletionListener[Unit](_ => linesReader.close())
            )

            linesReader.map(
              line =>
                new String(
                  line.getBytes,
                  0,
                  line.getLength,
                  parser.options.charset
                )
            )
          }

          val filteredLines = filterCommentEmptyAndFasta(lines)

          val rowConverter = new GffRowToInternalRowConverter(requiredSchema)

          filteredLines.map(l => rowConverter.convertRow(parser.parse(l)))

      }
    }
  }
}

object GffFileFormat {

  def createBaseDataset(
      sparkSession: SparkSession,
      inputPaths: Seq[FileStatus],
      options: CSVOptions): DataFrame = {

    val paths = inputPaths.map(_.getPath.toString)

    sparkSession.baseRelationToDataFrame(
      DataSource
        .apply(
          sparkSession,
          paths = paths,
          userSpecifiedSchema = Some(gffCsvSchema),
          className = classOf[CSVFileFormat].getName,
          options = options.parameters
        )
        .resolveRelation(checkFilesExist = false)
    )
  }

  def updateAttributesToken(
      currentToken: ParsedAttributesToken,
      attrStr: String): ParsedAttributesToken = {

    val attributes = attrStr.split(ATTRIBUTES_DELIMITER)
    val updatedSep: Char = if (currentToken.sep.isEmpty) {
      if (attributes(0).contains(GFF3_TAG_VALUE_DELIMITER)) {
        GFF3_TAG_VALUE_DELIMITER
      } else {
        GTF_TAG_VALUE_DELIMITER
      }
    } else {
      currentToken.sep.get
    }

    var i = 0
    var updatedTags = currentToken.tags

    while (i < attributes.length) {
      updatedTags += attributes(i).takeWhile(_ != updatedSep).trim
      i += 1
    }

    ParsedAttributesToken(Option(updatedSep), updatedTags)
  }

  def filterCommentEmptyAndFasta(iter: Iterator[String]): Iterator[String] = {
    iter.filter { line =>
      val trimmed = line.trim
      trimmed.nonEmpty &&
      !trimmed.startsWith(COMMENT_IDENTIFIER) &&
      !trimmed.startsWith(CONTIG_IDENTIFIER) &&
      !(trimmed.matches(FASTA_LINE_REGEX))
    }
  }

  case class ParsedAttributesToken(sep: Option[Char], tags: Set[String])

  private[gff] val gffCsvSchema = StructType(
    gffBaseSchema
      .fields
      .toSeq
      .map(f => StructField(f.name, StringType)) :+ attributesField
  )

  private[gff] val COLUMN_DELIMITER = "\t"
  private[gff] val ATTRIBUTES_DELIMITER = ";"
  private[gff] val GFF3_TAG_VALUE_DELIMITER = '='
  private[gff] val GTF_TAG_VALUE_DELIMITER = ' '
  private[gff] val COMMENT_IDENTIFIER = "#"
  private[gff] val CONTIG_IDENTIFIER = ">"
  private[gff] val FASTA_LINE_REGEX = "(?i)[acgt]*"
  private[gff] val NULL_IDENTIFIER = "."
  private[gff] val ARRAY_DELIMITER = ","

}
