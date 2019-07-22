package com.databricks.hls.transformers

import java.io.{Closeable, InputStream, OutputStream, PrintWriter}
import java.util.ServiceLoader

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.SQLUtils.dataTypesEqualExceptNullability
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.hls.DataFrameTransformer
import com.databricks.hls.common.Named

class PipeTransformer extends DataFrameTransformer {
  override def name: String = "pipe"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {
    new PipeTransformerImpl(df, options).transform()
  }

  // Implementation is in an inner class to avoid passing options to private methods
  private class PipeTransformerImpl(df: DataFrame, options: Map[String, String]) {
    import PipeTransformer._

    private def getInputFormatter: InputFormatter = {
      val inputFormatterStr = options.get(INPUT_FORMATTER_KEY)
      inputFormatterStr
        .flatMap(lookupInputFormatterFactory)
        .getOrElse {
          throw new IllegalArgumentException(
            s"Could not find an input formatter for $inputFormatterStr")
        }
        .makeInputFormatter(df, options)
    }

    private def getOutputFormatter: OutputFormatter = {
      val outputFormatterStr = options.get(OUTPUT_FORMATTER_KEY)
      outputFormatterStr
        .flatMap(lookupOutputFormatterFactory)
        .getOrElse {
          throw new IllegalArgumentException(
            s"Could not find an output formatter for $outputFormatterStr")
        }
        .makeOutputFormatter(options)
    }

    private def getCmd: Seq[String] = {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val str =
        options.getOrElse(CMD_KEY, throw new IllegalArgumentException("Must specify a command"))
      mapper.readValue(str, classOf[Seq[String]])
    }

    def transform(): DataFrame = {
      val inputFormatter = getInputFormatter
      val outputFormatter = getOutputFormatter
      val cmd = getCmd
      val env = options.collect {
        case (k, v) if k.startsWith(ENV_PREFIX) =>
          (k.stripPrefix(ENV_PREFIX), v)
      }
      Piper.pipe(inputFormatter, outputFormatter, cmd, env, df)
    }
  }
}

object PipeTransformer {
  private val CMD_KEY = "cmd"
  private val INPUT_FORMATTER_KEY = "inputFormatter"
  private val OUTPUT_FORMATTER_KEY = "outputFormatter"
  private val ENV_PREFIX = "env_"

  private def lookupInputFormatterFactory(name: String): Option[InputFormatterFactory] =
    synchronized {
      inputFormatterLoader.reload()
      inputFormatterLoader.iterator().asScala.find(_.name == name)
    }

  private def lookupOutputFormatterFactory(name: String): Option[OutputFormatterFactory] =
    synchronized {
      outputFormatterLoader.reload()
      outputFormatterLoader.iterator().asScala.find(_.name == name)
    }

  private lazy val inputFormatterLoader = ServiceLoader.load(classOf[InputFormatterFactory])
  private lazy val outputFormatterLoader = ServiceLoader.load(classOf[OutputFormatterFactory])
}

trait InputFormatter extends Serializable with Closeable {

  /**
   * Initialize the input formatter based on the outstream (i.e., the subprocess's stdout).
   *
   * This method is called per-partition, so all non-serializable initialization should happen
   * here.
   */
  def init(stream: OutputStream): Unit

  /**
   * Write a DataFrame record to the subprocess's stdout stream.
   * @param record
   */
  def write(record: InternalRow): Unit

  /**
   * Write a dummy dataset to the subprocess's stdout stream. This method is used for schema
   * inference -- the output formatter must be able to determine the output schema from the
   * subprocess's output for this dataset.
   */
  def writeDummyDataset(): Unit

  def close(): Unit
}

trait InputFormatterFactory extends Named {
  def makeInputFormatter(df: DataFrame, options: Map[String, String]): InputFormatter
}

trait OutputFormatter extends Serializable {

  /**
   * Determine the output schema based on the subprocess's stdout stream in response to the
   * input formatter's dummy dataset.
   *
   * @param stream The subprocess's stdout stream
   */
  def outputSchema(stream: InputStream): StructType

  /**
   * Construct an iterator of output rows from the subprocess's stdout stream in response to the
   * real data. The schema of each row must match the `schema`.
   * @param schema The output schema, as determined by this formatter's `outputSchema` method. All
   *               rows returned by the iterator must match this schema.
   * @param stream The subprocess's stdout stream
   * @return An iterator of [[InternalRow]]s with schema `schema`
   */
  def makeIterator(schema: StructType, stream: InputStream): Iterator[InternalRow]
}

trait OutputFormatterFactory extends Named {
  def makeOutputFormatter(options: Map[String, String]): OutputFormatter
}

/**
 * A simple input formatter that writes each row as a string.
 */
class UTF8TextInputFormatter() extends InputFormatter {

  private var writer: PrintWriter = _

  override def init(stream: OutputStream): Unit = {
    writer = new PrintWriter(stream)
  }

  override def write(record: InternalRow): Unit = {
    if (!record.isNullAt(0)) {
      writer.println(record.getUTF8String(0)) // scalastyle:ignore
    }
  }

  override def writeDummyDataset(): Unit = {}

  override def close(): Unit = {
    writer.close()
  }
}

class UTF8TextInputFormatterFactory extends InputFormatterFactory {
  override def name: String = "text"

  override def makeInputFormatter(df: DataFrame, options: Map[String, String]): InputFormatter = {
    require(df.schema.length == 1, "Input dataframe must have one column,")
    require(
      dataTypesEqualExceptNullability(df.schema.head.dataType, StringType),
      "Input dataframe must have one string column.")
    new UTF8TextInputFormatter
  }
}

/**
 * A simple output formatter that returns each line of output as a String field.
 */
class UTF8TextOutputFormatter() extends OutputFormatter {
  override def outputSchema(stream: InputStream): StructType =
    StructType(
      Seq(
        StructField("text", StringType)
      ))

  override def makeIterator(schema: StructType, stream: InputStream): Iterator[InternalRow] = {
    IOUtils.lineIterator(stream, "UTF-8").asScala.map { s =>
      new GenericInternalRow(Array(UTF8String.fromString(s)): Array[Any])
    }
  }
}

class UTF8TextOutputFormatterFactory extends OutputFormatterFactory {
  override def name: String = "text"

  override def makeOutputFormatter(options: Map[String, String]): OutputFormatter = {
    new UTF8TextOutputFormatter
  }
}
