package org.projectglow.core.transformers.pipe

import java.io.{Closeable, InputStream, OutputStream}
import java.util.ServiceLoader

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.projectglow.core.DataFrameTransformer
import org.projectglow.core.common.Named
import org.projectglow.core.common.logging._
import org.projectglow.core.transformers.util.SnakeCaseMap

class PipeTransformer extends DataFrameTransformer with HlsUsageLogging {
  override def name: String = "pipe"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {
    new PipeTransformerImpl(df, options).transform()
  }

  // Implementation is in an inner class to avoid passing options to private methods
  private class PipeTransformerImpl(df: DataFrame, options: Map[String, String]) {

    import PipeTransformer._

    private def getInputFormatter: InputFormatter = {
      val inputFormatterStr = options.get(INPUT_FORMATTER_KEY)
      val inputFormatterOptions = options.collect {
        case (k, v) if k.startsWith(INPUT_FORMATTER_PREFIX) =>
          (k.stripPrefix(INPUT_FORMATTER_PREFIX), v)
      }

      inputFormatterStr
        .flatMap(lookupInputFormatterFactory)
        .getOrElse {
          throw new IllegalArgumentException(
            s"Could not find an input formatter for $inputFormatterStr")
        }
        .makeInputFormatter(df, new SnakeCaseMap(inputFormatterOptions))
    }

    private def getOutputFormatter: OutputFormatter = {
      val outputFormatterStr = options.get(OUTPUT_FORMATTER_KEY)
      val outputFormatterOptions = options.collect {
        case (k, v) if k.startsWith(OUTPUT_FORMATTER_PREFIX) =>
          (k.stripPrefix(OUTPUT_FORMATTER_PREFIX), v)
      }

      outputFormatterStr
        .flatMap(lookupOutputFormatterFactory)
        .getOrElse {
          throw new IllegalArgumentException(
            s"Could not find an output formatter for $outputFormatterStr")
        }
        .makeOutputFormatter(new SnakeCaseMap(outputFormatterOptions))
    }

    private def getCmd: Seq[String] = {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val str =
        options.getOrElse(CMD_KEY, throw new IllegalArgumentException("Must specify a command"))
      mapper.readValue(str, classOf[Seq[String]])
    }

    def transform(): DataFrame = {

      val pipeToolSet = Array(
        "saige",
        "plink",
        "bcftools",
        "samtools",
        "grep",
        "cat"
      )

      val cmd = getCmd

      // record the pipe event along with tools of interest which maybe called using it.
      // TODO: More tools to be added
      val toolInPipe = Map(
        HlsBlobKeys.PIPE_CMD_TOOL ->
        pipeToolSet.foldLeft(Array[String]())(
          (a, b: String) =>
            if (cmd.exists(_.toLowerCase.contains(b))) {
              a :+ b
            } else {
              a
            }
        )
      )

      recordHlsUsage(
        HlsMetricDefinitions.EVENT_HLS_USAGE,
        Map(
          HlsTagDefinitions.TAG_EVENT_TYPE -> HlsTagValues.EVENT_PIPE
        ),
        blob = hlsJsonBuilder(toolInPipe)
      )

      val inputFormatter = getInputFormatter
      val outputFormatter = getOutputFormatter
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
  private val INPUT_FORMATTER_PREFIX = "in_"
  private val OUTPUT_FORMATTER_PREFIX = "out_"

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

  def close(): Unit
}

trait InputFormatterFactory extends Named {
  def makeInputFormatter(df: DataFrame, options: Map[String, String]): InputFormatter
}

trait OutputFormatter extends Serializable {

  /**
   * Construct an iterator of output rows from the subprocess's stdout stream in response to the
   * real data.
   * @param stream The buffered subprocess's stdout stream
   * @return An iterator consisting of the schema followed by [[InternalRow]]s with the schema
   */
  def makeIterator(stream: InputStream): Iterator[Any]
}

trait OutputFormatterFactory extends Named {
  def makeOutputFormatter(options: Map[String, String]): OutputFormatter
}
