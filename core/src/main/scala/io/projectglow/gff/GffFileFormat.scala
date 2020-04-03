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

import com.google.common.io.LittleEndianDataInputStream
import com.univocity.parsers.csv.CsvParser
import io.projectglow.common.logging.{HlsEventRecorder, HlsTagValues}
import io.projectglow.common.{CommonOptions, CompressionUtils, FeatureSchemas, GlowLogging, VCFOptions, VariantSchemas}
import io.projectglow.sql.util.{HadoopLineIterator, SerializableConfiguration}
import io.projectglow.vcf.{SchemaDelegate, TabixIndexHelper, VCFFileFormat, VCFIteratorDelegate, VariantContextToInternalRowConverter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.csv.TextInputCSVDataSource.{createBaseDataset, inferFromDataset}
import org.apache.spark.sql.execution.datasources.csv.{CSVDataSource, CSVFileFormat, CSVOptions, CSVUtils, TextInputCSVDataSource, UnivocityParser}
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.datasources.{DataSource, FailureSafeParser, FileFormat, HadoopFileLinesReader, OutputWriterFactory, PartitionedFile, TextBasedFileFormat}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import scala.collection.JavaConverters._

import io.projectglow.common.FeatureSchemas._
import io.projectglow.common.VariantSchemas.{alleleOneField, alleleTwoField, bimSchema, contigNameField, positionField, startField, variantIdField}
import io.projectglow.gff.GffFileFormat._

import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.catalyst.expressions.{Add, BoundReference, Cast, CreateArray, EqualTo, If, Length, Literal, MutableProjection, Subtract}


class GffFileFormat extends TextBasedFileFormat
    with DataSourceRegister with HlsEventRecorder with GlowLogging {

  var codecFactory: CompressionCodecFactory = _

  override def shortName(): String = "gff"

  /**
   * This is very similar to [[TextBasedFileFormat.isSplitable()]], but with additional check
   * for files that may or may not be block gzipped.
   */
  override def isSplitable(
    sparkSession: SparkSession,
    options: Map[String, String],
    path: Path): Boolean = {
    if (codecFactory == null) {
      codecFactory = new CompressionCodecFactory(
        CompressionUtils.hadoopConfWithBGZ(sparkSession.sessionState.newHadoopConf())
      )
    }

    // Note: we check if a file is gzipped vs block gzipped during reading, so this will be true
    // for .gz files even if they aren't actually splittable
    codecFactory.getCodec(path).isInstanceOf[SplittableCompressionCodec]
  }


  // With aggregate
  override def inferSchema(
    sparkSession: SparkSession,
    options: Map[String, String],
    files: Seq[FileStatus]): Option[StructType] = {

    val paths = files.map(_.getPath.toString)
    val optionsWithDelimiter = options + ("sep" -> "\t")

    val parsedOptions = new CSVOptions(optionsWithDelimiter,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone)

    val csv = GffFileFormat.createBaseDataset(sparkSession, files, parsedOptions)
      .select(attributesField.name)
      .filter(col(attributesField.name).isNotNull)

    // Use of ParsedAttributesToken helps update sep as soon as detected and avoid looking into
    // attributes array again to detect the separator when it is detected once.
    val attributesTokenZero = ParsedAttributesToken(None, Set[String]())

    val attributesToken = csv.queryExecution.toRdd.aggregate(attributesTokenZero)(
      { (t, r) =>
        val attributes = r.getString(0).split(";")
        GffFileFormat.updateAttributesToken(t, attributes)
      },
      { (t1, t2) =>
        ParsedAttributesToken(
          t1.sep.orElse(t2.sep.orElse(None)),
          t1.tags ++ t2.tags
        )
      }
    )

    // Separate base from others then merge.
    val officialAttributeFields = gffOfficialAttributeFields.foldLeft(Seq[StructField]()) {
      (s, f) =>
        if (attributesToken.tags.map(_.toLowerCase).contains(f.name.toLowerCase)) s :+ f else s
    }

    val remainingTags = attributesToken.tags.filter(
      t => !officialAttributeFields.map(_.name.toLowerCase).contains(t.toLowerCase)
    )

    val unofficialAttributeFields = remainingTags.foldLeft(Seq[StructField]()) {
      (s, t) => s :+ StructField(t, StringType)
    }

    Option(StructType(officialAttributeFields ++ unofficialAttributeFields))

    Option(gffBaseSchema)
  }


  override def prepareWrite(
    sparkSession: SparkSession,
    job: Job,
    options: Map[String, String],
    dataSchema: StructType): OutputWriterFactory = {
    throw new UnsupportedOperationException(
      "GFF data source does not support writing."
    )
  }


  override def buildReader(
    spark: SparkSession,
    dataSchema: StructType,
    partitionSchema: StructType,
    requiredSchema: StructType,
    filters: Seq[Filter],
    options: Map[String, String],
    hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    // TODO: WIP: Ignore this function for now

    // TODO: record vcfRead event in the log along with the options


    val serializableConf = new SerializableConfiguration(
      CompressionUtils.hadoopConfWithBGZ(hadoopConf)
    )


    Seq("sep", "delimiter").foreach { s =>
      val currentDelimiter = options.get(s)
      if (currentDelimiter.nonEmpty && currentDelimiter.get != "\t") {
        logger.info(s"""The separator must be tab. The "$s" option was ignored.""")
      }
    }

    val optionsWithDelimiter = options + ("sep" -> "\u0009")

    val parsedOptions = new CSVOptions(
      optionsWithDelimiter,
      spark.sessionState.conf.csvColumnPruning,
      spark.sessionState.conf.sessionLocalTimeZone
    )

    val caseSensitive = spark.sessionState.conf.caseSensitiveAnalysis
    val columnPruning = spark.sessionState.conf.csvColumnPruning



    partitionedFile => {
      val path = new Path(partitionedFile.filePath)
      val hadoopFs = path.getFileSystem(serializableConf.value)

      // Get the file offset=(startPos,endPos) that must be read from this partitionedFile.
      // Currently only one offset is generated for each partitionedFile.
      // val offset = Option((0L, partitionedFile.length))

      val isGzip = CompressionUtils.isGzip(partitionedFile, serializableConf.value)

      val offset: Option[(Long, Long)] = if (isGzip && partitionedFile.start == 0) {
      // Reading gzip file from beginning to end
        val fileLength = hadoopFs.getFileStatus(path).getLen
        Some((0, fileLength))
      } else if (isGzip) {
        // Skipping gzip file because task starts in the middle of the file
        None
      } else {
        Some((partitionedFile.start, partitionedFile.start + partitionedFile.length))
      }

      offset match {
        case None =>
          Iterator.empty

        case Some((startPos, endPos)) =>

          // need to read all as strings, then project to correct types
          val parser = new UnivocityParser(
            gffCsvSchema,
            gffCsvSchema,
            parsedOptions)

          val lines = {
            val linesReader = new HadoopLineIterator(
              partitionedFile.filePath,
              startPos,
              endPos - startPos,
              None,
              serializableConf.value
            )

            Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))

            linesReader.map(line =>
              new String(line.getBytes, 0, line.getLength, parser.options.charset)
            )
          }

          val filteredLines = filterCommentEmptyAndFasta(lines)

          val projection = makeMutableProjection(requiredSchema)


          filteredLines.map(parser.parse)
            .map(projection(_)) // without projection the reader is unable to cast types
        // form strings to int, long, or double in requiredSchema

      }
    }
  }
}

object GffFileFormat {

  def makeMutableProjection(schema: StructType): MutableProjection = {
    val expressions =
      schema.map {
        case f if structFieldsEqualExceptNullability(f, seqIdField) |
          structFieldsEqualExceptNullability(f, sourceField) |
          structFieldsEqualExceptNullability(f, typeField) |
          structFieldsEqualExceptNullability(f, attributesField)
        => makeGffBoundReference(f)

        case f if structFieldsEqualExceptNullability(f, startField) |
          structFieldsEqualExceptNullability(f, endField) |
          structFieldsEqualExceptNullability(f, scoreField)
        => Cast(makeGffBoundReference(f), f.dataType)

        case f if structFieldsEqualExceptNullability(f, strandField) |
          structFieldsEqualExceptNullability(f, phaseField) =>
          If(EqualTo(makeGffBoundReference(f), Literal(".")),
            Literal(null, f.dataType),
            Cast(makeGffBoundReference(f), f.dataType))

        case f => Literal(null, f.dataType)
      }
    GenerateMutableProjection.generate(expressions)
  }

  def makeGffBoundReference(f: StructField): BoundReference = {
    val idx = gffCsvSchema.fieldIndex(f.name)
    BoundReference(idx, gffCsvSchema.fields(idx).dataType, gffCsvSchema.fields(idx).nullable)
  }

  def createBaseDataset(
    sparkSession: SparkSession,
    inputPaths: Seq[FileStatus],
    options: CSVOptions): DataFrame = {

    import FeatureSchemas._
    val paths = inputPaths.map(_.getPath.toString)

    sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = paths,
        userSpecifiedSchema = Some(gffCsvSchema),
        className = classOf[CSVFileFormat].getName,
        options = options.parameters
      ).resolveRelation(checkFilesExist = false))
  }

  def updateAttributesToken(
    currentToken: ParsedAttributesToken,
    attributes: Array[String]): ParsedAttributesToken = {

    val updatedSep: Char = if (currentToken.sep.isEmpty) {
      if (attributes(0).contains('=')) '=' else ' '
    } else {
      currentToken.sep.get
    }

    var i = 0
    var updatedTags = currentToken.tags

    while (i < attributes.length) {
      updatedTags += attributes(i).takeWhile(_ != updatedSep)
      i += 1
    }

    ParsedAttributesToken(Option(updatedSep), updatedTags)
  }

  def filterCommentEmptyAndFasta(iter: Iterator[String]): Iterator[String] = {
    iter.filter { line =>
    val trimmed = line.trim
      trimmed.nonEmpty &&
        !trimmed.startsWith("#") &&
        !trimmed.startsWith(">") &&
        !(trimmed.matches("(?i)[acgt]*"))
    }
  }

  /*
  def parseLines(
    lines: Iterator[String],
    parser: UnivocityParser,
    schema: StructType): Iterator[InternalRow] = {
    val options = parser.options

    val safeParser = new FailureSafeParser[String](
      input => Seq(parser.parse(input)),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)
    safeParser.parse(lines)
  }
*/
  case class ParsedAttributesToken(sep: Option[Char], tags: Set[String])

  private val gffCsvSchema = StructType(
    gffBaseSchema.fields.toSeq.map(f =>
      StructField(f.name, StringType)) :+ attributesField
  )

}
