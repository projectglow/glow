package org.projectglow.core.transformers.pipe

import java.io._
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLUtils}
import org.projectglow.common.HLSLogging
import org.projectglow.core.common.HLSLogging

import com.databricks.hls.common.HLSLogging

/**
 * Based on Spark's PipedRDD with the following modifications:
 * - Act only on DataFrames instead of generic RDDs
 * - Use the input and output formatters to determine output schema
 * - Use the input and output formatters to return a DataFrame
 */
private[databricks] object Piper extends HLSLogging {
  private val cachedRdds = mutable.ListBuffer[RDD[_]]()

  def clearCache(): Unit = cachedRdds.synchronized {
    cachedRdds.foreach(_.unpersist())
    cachedRdds.clear()
  }

  // Pipes a single row of the input DataFrame to get the output schema before piping all of it.
  def pipe(
      informatter: InputFormatter,
      outputformatter: OutputFormatter,
      cmd: Seq[String],
      env: Map[String, String],
      df: DataFrame): DataFrame = {

    logger.info(s"Beginning pipe with cmd $cmd")

    // Each partition consists of an iterator with the schema, followed by [[InternalRow]]s with the
    // schema
    val schemaInternalRowRDD = df
      .queryExecution
      .toRdd
      .mapPartitions { it =>
        new PipeIterator(cmd, env, it, informatter, outputformatter)
      }
      .cache()

    cachedRdds.synchronized {
      cachedRdds.append(schemaInternalRowRDD)
    }

    val schemaSeq = schemaInternalRowRDD.mapPartitions { it =>
      if (it.hasNext) {
        Iterator(it.next.asInstanceOf[StructType])
      } else {
        Iterator.empty
      }
    }.collect.distinct

    if (schemaSeq.length != 1) {
      throw new IllegalStateException(
        s"Cannot infer schema: saw ${schemaSeq.length} distinct schemas.")
    }

    val schema = schemaSeq.head
    val internalRowRDD = schemaInternalRowRDD.mapPartitions { it =>
      it.drop(1).asInstanceOf[Iterator[InternalRow]]
    }

    SQLUtils.internalCreateDataFrame(df.sparkSession, internalRowRDD, schema, isStreaming = false)
  }
}

private[databricks] class ProcessHelper(
    cmd: Seq[String],
    environment: Map[String, String],
    inputFn: OutputStream => Unit,
    context: TaskContext)
    extends HLSLogging {

  private val childThreadException = new AtomicReference[Throwable](null)
  private var process: Process = _

  def startProcess(): BufferedInputStream = {

    val pb = new ProcessBuilder(cmd.asJava)
    val pbEnv = pb.environment()
    environment.foreach { case (k, v) => pbEnv.put(k, v) }
    process = pb.start()

    new Thread(s"${ProcessHelper.STDIN_WRITER_THREAD_PREFIX} for $cmd") {
      override def run(): Unit = {
        SQLUtils.setTaskContext(context)
        val out = process.getOutputStream
        try {
          inputFn(out)
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          out.close()
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

    new BufferedInputStream(process.getInputStream)
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
  val STDIN_WRITER_THREAD_PREFIX = "stdin writer"
  val STDERR_READER_THREAD_PREFIX = "stderr reader"
}

class PipeIterator(
    cmd: Seq[String],
    environment: Map[String, String],
    input: Iterator[InternalRow],
    inputFormatter: InputFormatter,
    outputFormatter: OutputFormatter)
    extends Iterator[Any] {

  private val processHelper = new ProcessHelper(cmd, environment, writeInput, TaskContext.get)
  private val inputStream = processHelper.startProcess()
  private val baseIterator = outputFormatter.makeIterator(inputStream)

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

  override def next(): Any = baseIterator.next()
}
