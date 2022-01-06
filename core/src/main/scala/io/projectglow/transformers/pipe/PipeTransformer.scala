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

package io.projectglow.transformers.pipe

import java.io.{Closeable, InputStream, OutputStream}
import java.util.ServiceLoader

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow

import io.projectglow.DataFrameTransformer
import io.projectglow.common.Named
import io.projectglow.common.logging._
import io.projectglow.transformers.util.SnakeCaseMap

class PipeTransformer extends DataFrameTransformer {
  override def name: String = "pipe"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {
    new PipeTransformerImpl(df, options).transform()
  }

  // Implementation is in an inner class to avoid passing options to private methods
  private class PipeTransformerImpl(df: DataFrame, options: Map[String, String])
      extends HlsEventRecorder {

    import PipeTransformer._

    private def getInputFormatter: InputFormatter[_] = {
      val inputFormatterStr = options.getOrElse(
        INPUT_FORMATTER_KEY,
        throw new IllegalArgumentException("Missing pipe input formatter."))

      val inputFormatterOptions = options.collect {
        case (k, v) if k.startsWith(INPUT_FORMATTER_PREFIX) =>
          (k.stripPrefix(INPUT_FORMATTER_PREFIX), v)
      }

      lookupInputFormatterFactory(inputFormatterStr).getOrElse {
        throw new IllegalArgumentException(
          s"Could not find an input formatter for $inputFormatterStr")
      }.makeInputFormatter(df, new SnakeCaseMap(inputFormatterOptions))
    }

    private def getOutputFormatter: OutputFormatter = {
      val outputFormatterStr = options.getOrElse(
        OUTPUT_FORMATTER_KEY,
        throw new IllegalArgumentException("Missing pipe output formatter."))

      val outputFormatterOptions = options.collect {
        case (k, v) if k.startsWith(OUTPUT_FORMATTER_PREFIX) =>
          (k.stripPrefix(OUTPUT_FORMATTER_PREFIX), v)
      }

      lookupOutputFormatterFactory(outputFormatterStr).getOrElse {
        throw new IllegalArgumentException(
          s"Could not find an output formatter for $outputFormatterStr")
      }.makeOutputFormatter(new SnakeCaseMap(outputFormatterOptions))
    }

    private def getQuarantineLocation: Option[String] =
      options.get(QUARANTINE_TABLE_KEY)
    private def getQuarantineFlavor: Option[String] =
      options.get(QUARANTINE_FLAVOR_KEY)

    private def getCmd: Seq[String] = {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val str =
        options.getOrElse(CMD_KEY, throw new IllegalArgumentException("Must specify a command"))
      mapper.readValue(str, classOf[Seq[String]])
    }

    private def getLogOptions(cmd: Seq[String]): Map[String, Any] = {
      // TODO: More tools to be added
      val pipeToolSet = Array(
        "saige",
        "plink",
        "bcftools",
        "samtools",
        "grep",
        "cat"
      )

      Map(
        LOGGING_BLOB_KEY ->
        pipeToolSet
          .foldLeft(Array[String]())(
            (a, b: String) =>
              if (cmd.exists(_.toLowerCase.contains(b))) {
                a :+ b
              } else {
                a
              }
          )
          .mkString(",")
      )
    }

    def transform(): DataFrame = {
      val cmd = getCmd

      // record the pipe event along with tools of interest which maybe called using it.
      recordHlsEvent(HlsTagValues.EVENT_PIPE, getLogOptions(cmd))

      val inputFormatter = getInputFormatter
      val outputFormatter = getOutputFormatter
      val quarantineLocation = getQuarantineLocation
      val quarantineFlavor = getQuarantineFlavor
      val quarantine = quarantineLocation.flatMap { a =>
        quarantineFlavor.map { b =>
          (a, b)
        }
      }
      val env = options.collect {
        case (k, v) if k.startsWith(ENV_PREFIX) =>
          (k.stripPrefix(ENV_PREFIX), v)
      }

      Piper.pipe(inputFormatter, outputFormatter, cmd, env, df, quarantine)
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
  private val QUARANTINE_TABLE_KEY = "quarantineTable"
  private val QUARANTINE_FLAVOR_KEY = "quarantineFlavor"

  val LOGGING_BLOB_KEY = "pipeCmdTool"

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

trait InputFormatter[A] extends Serializable with Closeable {

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
   * Converts DataFrame record into something writable.
   * @param record
   */
  def value(record: InternalRow): A

  def close(): Unit
}

trait InputFormatterFactory extends Named {
  def makeInputFormatter(df: DataFrame, options: Map[String, String]): InputFormatter[_]
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
