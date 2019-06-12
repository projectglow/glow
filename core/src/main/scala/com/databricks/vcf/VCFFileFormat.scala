package com.databricks.vcf

import java.io.BufferedInputStream

import scala.collection.JavaConverters._

import htsjdk.samtools.ValidationStringency
import htsjdk.samtools.util.BlockCompressedInputStream
import htsjdk.tribble.readers.{AsciiLineReader, AsciiLineReaderIterator}
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._
import org.broadinstitute.hellbender.tools.walkers.genotyper.GenotypeAssignmentMethod
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils
import org.seqdoop.hadoop_bam.util.{BGZFEnhancedGzipCodec, DatabricksBGZFOutputStream}

import com.databricks.hls.common.{HLSLogging, WithUtils}
import com.databricks.hls.sql.util.{EncoderUtils, HadoopLineIterator, SerializableConfiguration}

class VCFFileFormat extends TextBasedFileFormat with DataSourceRegister with HLSLogging {
  var codecFactory: CompressionCodecFactory = _

  override def shortName(): String = "com.databricks.vcf"

  /**
   * This is very similar to [[TextBasedFileFormat.isSplitable()]], but with additional check
   * for files that may or may not be block gzipped.
   *
   * The check for BGZIP compression is adapted from [[org.seqdoop.hadoop_bam.VCFInputFormat]].
   */
  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = {
    if (codecFactory == null) {
      codecFactory = new CompressionCodecFactory(
        VCFFileFormat.hadoopConfWithBGZ(sparkSession.sessionState.newHadoopConf())
      )
    }

    codecFactory.getCodec(path) match {
      case null => true
      case _: BGZFEnhancedGzipCodec =>
        val fs = path.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
        WithUtils.withCloseable(fs.open(path)) { is =>
          val buffered = new BufferedInputStream(is)
          BlockCompressedInputStream.isValidFile(buffered)
        }
      case c => c.isInstanceOf[SplittableCompressionCodec]
    }
  }

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    Option(SchemaDelegate.makeDelegate(options).schema(sparkSession, files))
  }

  // Current limitation: VCF will only contain the samples listed in the first row written
  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {

    options.get("compression").foreach { compressionOption =>
      if (codecFactory == null) {
        codecFactory =
          new CompressionCodecFactory(VCFFileFormat.hadoopConfWithBGZ(job.getConfiguration))
      }

      val codec = codecFactory.getCodecByName(compressionOption)
      CompressionCodecs.setCodecConfiguration(job.getConfiguration, codec.getClass.getName)
    }

    new VCFOutputWriterFactory(options)
  }

  override def buildReader(
      spark: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    val serializableConf =
      new SerializableConfiguration(VCFFileFormat.hadoopConfWithBGZ(hadoopConf))

    partitionedFile => {

      // In case of a partitioned file that only contains part of the header, codec.readActualHeader
      // will throw an error for a malformed header. We therefore allow the header reader to read
      // past the boundaries of the partitions; it will throw/return when done reading the header.
      val path = new Path(partitionedFile.filePath)
      val (header, codec) = VCFFileFormat.createVCFCodec(path, serializableConf.value)

      val reader = new HadoopLineIterator(
        partitionedFile.filePath,
        partitionedFile.start,
        partitionedFile.length,
        None,
        serializableConf.value
      )

      Option(TaskContext.get()).foreach { taskContext =>
        taskContext.addTaskCompletionListener[Unit] { _ =>
          reader.close()
        }
      }

      SchemaDelegate
        .makeDelegate(options)
        .toRows(header, requiredSchema, VCFIteratorDelegate.makeDelegate(options, reader, codec))
    }
  }

}

object VCFFileFormat {

  /**
   * Reads the header of a VCF file to generate an object representing the information in the header
   * and a derived [[VCFCodec]] that can parse the rest of the file.
   *
   * The logic to parse the header is adapted from [[org.seqdoop.hadoop_bam.VCFRecordReader]].
   */
  def createVCFCodec(path: Path, conf: Configuration): (VCFHeader, VCFCodec) = {
    val fs = path.getFileSystem(conf)
    WithUtils.withCloseable(fs.open(path)) { is =>
      val compressionCodec = new CompressionCodecFactory(conf).getCodec(path)
      val wrappedStream = if (compressionCodec != null) {
        val decompressor = CodecPool.getDecompressor(compressionCodec)
        compressionCodec.createInputStream(is, decompressor)
      } else {
        is
      }

      val vcfCodec = new VCFCodec()
      val reader = new AsciiLineReaderIterator(AsciiLineReader.from(wrappedStream))
      val header = vcfCodec
        .readActualHeader(reader)
      (header.asInstanceOf[VCFHeader], vcfCodec)
    }
  }

  /**
   * Adds BGZF support to an existing Hadoop conf.
   */
  def hadoopConfWithBGZ(conf: Configuration): Configuration = {
    val toReturn = new Configuration(conf)
    val bgzCodecs = Seq(
      "com.databricks.hls.sql.util.BGZFCodec",
      "org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec"
    )
    val codecs = toReturn
        .get("io.compression.codecs", "")
        .split(",")
        .filter(codec => codec.nonEmpty && !bgzCodecs.contains(codec)) ++ bgzCodecs
    toReturn.set("io.compression.codecs", codecs.mkString(","))
    toReturn
  }
}

case class VariantContextWrapper(vc: VariantContext, splitFromMultiallelic: Boolean)

private object VCFIteratorDelegate {
  def makeDelegate(
      options: Map[String, String],
      lineReader: Iterator[Text],
      codec: VCFCodec): AbstractVCFIterator = {
    if (options.get("splitToBiallelic").exists(_.toBoolean)) {
      new BiallelicVCFIterator(lineReader, codec)
    } else {
      new VCFIterator(lineReader, codec)
    }
  }
}

private[vcf] abstract class AbstractVCFIterator(codec: VCFCodec)
    extends Iterator[VariantContextWrapper] {

  def parseLine(line: String): VariantContext = {
    try {
      codec.decode(line)
    } catch {
      // The HTSJDK does not parse nan, only NaN
      case _: NumberFormatException => codec.decode(line.replaceAll("[nN][aA][nN]", "NaN"))
    }
  }
}

private[vcf] class VCFIterator(lineReader: Iterator[Text], codec: VCFCodec)
    extends AbstractVCFIterator(codec)
    with HLSLogging {

  private var nextVC: VariantContext = _

  override def hasNext: Boolean = {
    while (lineReader.hasNext && nextVC == null) {
      nextVC = parseLine(lineReader.next.toString)
    }

    nextVC != null
  }

  override def next(): VariantContextWrapper = {
    if (nextVC == null) {
      throw new NoSuchElementException("Called next on empty iterator")
    }
    val ret = nextVC
    nextVC = null
    VariantContextWrapper(ret, false)
  }
}

private[vcf] class BiallelicVCFIterator(lineReader: Iterator[Text], codec: VCFCodec)
    extends AbstractVCFIterator(codec)
    with HLSLogging {

  private var isSplit: Boolean = false
  private var nextVCs: Iterator[VariantContext] = Iterator.empty

  override def hasNext: Boolean = {
    while (lineReader.hasNext && !nextVCs.hasNext) {
      val vc = parseLine(lineReader.next.toString)

      // The codec returns null if it can't parse the line (i.e., if we're still in the header)
      if (vc != null) {
        val htsjdkVcList = GATKVariantContextUtils.splitVariantContextToBiallelics(
          vc,
          /* trimLeft */ true,
          GenotypeAssignmentMethod.BEST_MATCH_TO_ORIGINAL,
          /* keepOriginalChrCounts */ false
        )
        isSplit = htsjdkVcList.size() > 1
        nextVCs = htsjdkVcList.asScala.toIterator
      }
    }

    nextVCs.hasNext
  }

  override def next(): VariantContextWrapper = {
    if (!nextVCs.hasNext) {
      throw new NoSuchElementException("Called next on empty iterator")
    }
    VariantContextWrapper(nextVCs.next, isSplit)
  }
}

private[vcf] sealed trait SchemaDelegate {
  def schema(sparkSession: SparkSession, files: Seq[FileStatus]): StructType
  def toRows(
      header: VCFHeader,
      requiredSchema: StructType,
      iterator: Iterator[VariantContextWrapper]): Iterator[InternalRow]
}

/**
 * Abstracts out the parts of VCF parsing that depend on the output schema.
 */
private[vcf] object SchemaDelegate {

  def makeDelegate(options: Map[String, String]): SchemaDelegate = {
    val stringency = VCFOptionParser.getValidationStringency(options)
    val includeSampleId = options.get("includeSampleIds").exists(_.toBoolean)
    if (options.get("vcfRowSchema").exists(_.toBoolean)) {
      new VCFRowSchemaDelegate(stringency, includeSampleId)
    } else if (options.get("flattenInfoFields").exists(_.toBoolean)) {
      new FlattenedInfoDelegate(includeSampleId, stringency)
    } else {
      new NormalDelegate(includeSampleId, stringency)
    }
  }

  private def readHeaders(
      spark: SparkSession,
      files: Seq[FileStatus]): (Seq[VCFInfoHeaderLine], Seq[VCFFormatHeaderLine]) = {
    val serializableConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())

    spark.sparkContext
      .parallelize(files)
      .map { file =>
        val (header, _) = VCFFileFormat.createVCFCodec(file.getPath, serializableConf.value)

        (header.getInfoHeaderLines.asScala.toSeq, header.getFormatHeaderLines.asScala.toSeq)
      }
      .collect()
      .foldLeft((Seq.empty[VCFInfoHeaderLine], Seq.empty[VCFFormatHeaderLine])) {
        case ((allInfo, allFormat), (info, format)) =>
          (allInfo ++ info, allFormat ++ format)
      }
  }

  private class VCFRowSchemaDelegate(stringency: ValidationStringency, includeSampleIds: Boolean)
      extends SchemaDelegate {
    override def schema(sparkSession: SparkSession, files: Seq[FileStatus]): StructType = {
      ScalaReflection.schemaFor[VCFRow].dataType.asInstanceOf[StructType]
    }

    override def toRows(
        header: VCFHeader,
        requiredSchema: StructType,
        iterator: Iterator[VariantContextWrapper]): Iterator[InternalRow] = {
      val converter = new VariantContextToInternalRowConverter(
        header,
        requiredSchema,
        stringency,
        writeSampleIds = includeSampleIds
      )
      iterator.map { vc =>
        converter.convertRow(vc.vc, vc.splitFromMultiallelic)
      }
    }
  }

  private class NormalDelegate(includeSampleIds: Boolean, stringency: ValidationStringency)
      extends SchemaDelegate {
    override def schema(sparkSession: SparkSession, files: Seq[FileStatus]): StructType = {
      val (infoHeaders, formatHeaders) = readHeaders(sparkSession, files)
      VCFSchemaInferer.inferSchema(includeSampleIds, false, infoHeaders, formatHeaders)
    }

    override def toRows(
        header: VCFHeader,
        requiredSchema: StructType,
        iterator: Iterator[VariantContextWrapper]): Iterator[InternalRow] = {
      val converter = new VariantContextToInternalRowConverter(header, requiredSchema, stringency)
      iterator.map { vc =>
        converter.convertRow(vc.vc, vc.splitFromMultiallelic)
      }
    }
  }

  private class FlattenedInfoDelegate(includeSampleIds: Boolean, stringency: ValidationStringency)
      extends SchemaDelegate {

    override def schema(sparkSession: SparkSession, files: Seq[FileStatus]): StructType = {
      val (infoHeaders, formatHeaders) = readHeaders(sparkSession, files)
      VCFSchemaInferer.inferSchema(includeSampleIds, true, infoHeaders, formatHeaders)
    }

    override def toRows(
        header: VCFHeader,
        requiredSchema: StructType,
        iterator: Iterator[VariantContextWrapper]): Iterator[InternalRow] = {

      val converter = new VariantContextToInternalRowConverter(header, requiredSchema, stringency)
      iterator.map { vc =>
        converter.convertRow(vc.vc, vc.splitFromMultiallelic)
      }
    }
  }
}

private[databricks] class VCFOutputWriterFactory(options: Map[String, String])
    extends OutputWriterFactory {

  private val validationStringency = VCFOptionParser.getValidationStringency(options)

  def getRowConverter(dataSchema: StructType): InternalRowToHtsjdkConverter = {
    val lines = VCFFileWriter.getHeaderLines(VCFRowHeaderLines.allHeaderLines, options)
    DefaultInternalRowToHtsjdkConverter(dataSchema, lines, validationStringency)
  }

  override def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    val outputStream = CodecStreams.createOutputStream(context, new Path(path))
    DatabricksBGZFOutputStream.setWriteEmptyBlockOnClose(outputStream, true)

    new VCFFileWriter(outputStream, getRowConverter(dataSchema), writeHeader = true)
  }

  override def getFileExtension(context: TaskAttemptContext): String = {
    ".vcf" + CodecStreams.getCompressionExtension(context)
  }
}

private[databricks] object VCFOptionParser {
  def getValidationStringency(options: Map[String, String]): ValidationStringency = {
    val stringency = options.getOrElse("validationStringency", "SILENT").toUpperCase
    ValidationStringency.valueOf(stringency)
  }
}
