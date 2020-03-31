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
import io.projectglow.gff.GffDataSource
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
import org.apache.spark.sql.execution.datasources.{DataSource, FileFormat, HadoopFileLinesReader, OutputWriterFactory, PartitionedFile, TextBasedFileFormat}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StructField, StructType, StringType}
import scala.collection.JavaConverters._
import io.projectglow.common.FeatureSchemas._
import io.projectglow.gff.GffFileFormat._


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
      .select(attributesFieldName)
      .filter(col(attributesFieldName).isNotNull)

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
    val attributesSchema = Seq[StructField]()

    val finalSchema = StructType(
      attributesToken.tags.foldLeft(attributesSchema) { (s, t) =>

        val gffField = gffAttributesSchema.filter(_.name.toLowerCase == t.toLowerCase)

        if (gffField.nonEmpty) {
          gffField ++ s
        } else {
          s :+ StructField(t, StringType)
        }
      }
    )

    Option(finalSchema)
  }


  /*
// With accumulator
  override def inferSchema(
    sparkSession: SparkSession,
    options: Map[String, String],
    files: Seq[FileStatus]): Option[StructType] = {

    import FeatureSchemas._
    val paths = files.map(_.getPath.toString)
    val optionsWithDelimiter = options + ("sep" -> "\t")

    val parsedOptions = new CSVOptions(optionsWithDelimiter,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone)

    val csv = GffFileFormat.createBaseDataset(sparkSession, files, parsedOptions)
      .select(attributesFieldName)
        .filter(col(attributesFieldName).isNotNull)

    val attFields = sparkSession.sparkContext.collectionAccumulator[String]


    csv.queryExecution.toRdd.foreachPartition { it =>

      var sepOption = Option.empty[String]

      it.foreach { r =>
        val attributesArray = r.getString(0).split(";")
        if (sepOption.isEmpty) {
          if (attributesArray(0).contains("=")) {
            sepOption = Some("=")
          } else {
            sepOption = Some(" ")
          }
        }

        val attFieldsAsSet = attFields.value.asScala.toSet[String]
        val parsedTags = GffFileFormat.getParsedTags(attributesArray, sepOption.get)

        while (i < parsedTags.length) {
          if (!attFieldsAsSet.contains(parsedTags(i))
          i += 1
        }


       attFields = GffFileFormat.updateAttFieldsWithParsedTags(
         attFields,
         attributesArray,
         sepOption.get
       )

      }

    }

    val attributesSchema = Seq[StructField]()

    val finalSchema = StructType(
      attFields.foldLeft(attributesSchema) { (s, f) =>

        val gffField = gffAttributesSchema.filter(_.name.toLowerCase == f.toLowerCase)

        if (gffField.nonEmpty) {
          s ++ gffField
        } else {
          s :+ StructField(f, StringType)
        }
      }
    )

    Option(finalSchema)
  }
*/



/*
  //  val maybeFirstLine = CSVUtils.filterCommentAndEmpty(csv, parsedOptions).take(1).headOption
  //  inferFromDataset(sparkSession, csv, maybeFirstLine, parsedOptions)

    val csv = sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = paths,
        // userSpecifiedSchema = Some(FeatureSchemas.gffSchema),
        className = classOf[CSVFileFormat].getName
        // options = optionsWithDelimiter
      ).resolveRelation(checkFilesExist = true)
    )//.select(FeatureSchemas.attributesFieldName)

    println(csv.count())
//    csv.foreach { row =>
//      row.toSeq.foreach{col => println(col) }
//    }
*/

   // csv.show()
    // inferFromDataset(sparkSession, csv, maybeFirstLine, parsedOptions)
 //    Some(FeatureSchemas.gffSchema)








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

    // TODO:record vcfRead event in the log along with the options


    val serializableConf = new SerializableConfiguration(
      CompressionUtils.hadoopConfWithBGZ(hadoopConf)
    )

    /* Make a filtered interval by parsing the filters
     Filters are parsed even if useTabixIndex is disabled because the parser can help with
     variant skipping in the VCF iterator if there is no overlap with the filteredInterval,
     improving the performance benefiting from lazy loading of genotypes */

    //    val filteredSimpleInterval =
    //      TabixIndexHelper.makeFilteredInterval(filters, useFilterParser, useIndex)


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
      spark.sessionState.conf.sessionLocalTimeZone,
      spark.sessionState.conf.columnNameOfCorruptRecord)



    val caseSensitive = spark.sessionState.conf.caseSensitiveAnalysis
    val columnPruning = spark.sessionState.conf.csvColumnPruning

    partitionedFile => {
      val path = new Path(partitionedFile.filePath)


      // Get the file offset=(startPos,endPos) that must be read from this partitionedFile.
      // Currently only one offset is generated for each partitionedFile.
      // val offset = Option((0L, partitionedFile.length))
      // TODO: Add Tabix usage

      val parser = new UnivocityParser(
        dataSchema,
        requiredSchema,
        parsedOptions)

      CSVDataSource(parsedOptions).readFile(
        serializableConf.value,
        partitionedFile,
        parser,
        requiredSchema,
        dataSchema,
        caseSensitive,
        columnPruning)
    }
  }
}

object GffFileFormat {

  def createBaseDataset(
    sparkSession: SparkSession,
    inputPaths: Seq[FileStatus],
    options: CSVOptions): DataFrame = {

    import FeatureSchemas._
    val paths = inputPaths.map(_.getPath.toString)

    val gffBaseSchemaPlusAtributes = StructType(
      gffBaseSchema :+ StructField(attributesFieldName, StringType)
    )

    sparkSession.baseRelationToDataFrame(
      DataSource.apply(
        sparkSession,
        paths = paths,
        userSpecifiedSchema = Some(gffBaseSchemaPlusAtributes),
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

  case class ParsedAttributesToken(sep: Option[Char], tags: Set[String])
  /*

  def getParsedTags(
    attributes: Array[String],
    sep: String): Array[String] = {

    var i = 0

    val parsedTags = Array[String]()

    while (i < attributes.length) {
      parsedTags(i) = attributes(i).take(attributes(i).indexOf(sep))
      i += 1
    }
    parsedTags
  }

   */

}
