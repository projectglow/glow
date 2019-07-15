package com.databricks.hls.transformers

import java.io._
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.io.Source

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLUtils}

import com.databricks.hls.common.{HLSLogging, WithUtils}

/**
 * Based on Spark's PipedRDD with the following modifications:
 * - Act only on DataFrames instead of generic RDDs
 * - Use the input and output formatters to determine output schema
 * - Use the input and output formatters to return a DataFrame
 */
private[databricks] object Piper extends HLSLogging {
  def pipe(
      informatter: InputFormatter,
      outputformatter: OutputFormatter,
      cmd: Seq[String],
      env: Map[String, String],
      df: DataFrame): DataFrame = {

    logger.info(s"Beginning pipe with cmd $cmd")

    val spark = df.sparkSession
    val outputSchema = spark
      .range(1)
      .rdd
      .map { _ =>
        val writeFn: OutputStream => Unit = os => {
          WithUtils.withCloseable(informatter) { formatter =>
            formatter.init(os)
            formatter.writeDummyDataset()
          }
        }
        val helper = new ProcessHelper(cmd, env, writeFn)
        val ret = outputformatter.outputSchema(helper.startProcess())
        helper.waitForProcess()
        ret
      }
      .first()

    val mapped = df.queryExecution.toRdd.mapPartitions { it =>
      new PipeIterator(cmd, env, it, informatter, outputformatter, outputSchema)
    }

    SQLUtils.createDataFrame(df.sparkSession, mapped, outputSchema)
  }
}

private[databricks] class ProcessHelper(
    cmd: Seq[String],
    environment: Map[String, String],
    inputFn: OutputStream => Unit)
    extends HLSLogging {

  private val childThreadException = new AtomicReference[Throwable](null)
  private var process: Process = _
  def startProcess(): InputStream = {

    val pb = new ProcessBuilder(cmd.asJava)
    val pbEnv = pb.environment()
    environment.foreach { case (k, v) => pbEnv.put(k, v) }
    process = pb.start()

    new Thread(s"${ProcessHelper.STDIN_WRITER_THREAD_PREFIX} for $cmd") {
      override def run(): Unit = {
        try {
          inputFn(process.getOutputStream)
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          process.getOutputStream.close()
        }
      }
    }.start()

    new Thread(s"${ProcessHelper.STDERR_READER_THREAD_PREFIX} for $cmd") {
      override def run(): Unit = {
        val err = process.getErrorStream
        try {
          for (line <- Source.fromInputStream(err).getLines) {
            logger.info(s"Got stderr line")
            // scalastyle:off println
            System.err.println(line)
            // scalastyle:on println
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          err.close()
        }
      }
    }.start()

    process.getInputStream
  }

  def waitForProcess(): Int = {
    if (process == null) {
      throw new IllegalStateException(s"Process hasn't been started yet")
    }
    process.waitFor()
  }

  def propagateChildException(): Unit = {
    val t = childThreadException.get()
    if (t != null) {
      Option(process).foreach(_.destroy())
      throw t
    }
  }
}

object ProcessHelper {
  val STDIN_WRITER_THREAD_PREFIX = "stdin writer for"
  val STDERR_READER_THREAD_PREFIX = "stderr reader for"
}

class PipeIterator(
    cmd: Seq[String],
    environment: Map[String, String],
    input: Iterator[InternalRow],
    inputFormatter: InputFormatter,
    outputFormatter: OutputFormatter,
    outputSchema: StructType)
    extends Iterator[InternalRow] {

  private val processHelper = new ProcessHelper(cmd, environment, writeInput)
  private val inputStream = processHelper.startProcess()
  private val baseIterator = outputFormatter.makeIterator(outputSchema, inputStream)

  private def writeInput(stream: OutputStream): Unit = {
    try {
      inputFormatter.init(stream)
      input.foreach(inputFormatter.write)
    } finally {
      inputFormatter.close()
    }
  }

  override def hasNext: Boolean = {
    val result = if (baseIterator.hasNext) {
      true
    } else {
      val exitStatus = processHelper.waitForProcess()
      if (exitStatus != 0) {
        throw new IllegalStateException(s"Subprocess exited with status $exitStatus")
      }
      false
    }
    processHelper.propagateChildException()
    result
  }

  override def next(): InternalRow = baseIterator.next()
}
