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

import java.lang.{IllegalStateException => ISE}
import java.io._
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLUtils, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.CollectionAccumulator

import io.projectglow.common.{GlowLogging, WithUtils}

/**
 * Based on Spark's PipedRDD with the following modifications:
 * - Act only on DataFrames instead of generic RDDs
 * - Use the input and output formatters to determine output schema
 * - Use the input and output formatters to return a DataFrame
 */
private[projectglow] object Piper extends GlowLogging {
  private val cachedRdds = mutable.ListBuffer[RDD[_]]()

  def clearCache(): Unit = cachedRdds.synchronized {
    SparkSession.getActiveSession match {
      case None => // weird
      case Some(spark) =>
        cachedRdds.foreach { rdd =>
          if (rdd.sparkContext == spark.sparkContext) {
            rdd.unpersist()
          }
        }
    }
    cachedRdds.clear()
  }

  // Pipes a single row of the input DataFrame to get the output schema before piping all of it.
  def pipe(
      informatter: InputFormatter[_],
      outputformatter: OutputFormatter,
      cmd: Seq[String],
      env: Map[String, String],
      df: DataFrame,
      quarantineLocation: Option[(String, String)] = None): DataFrame = {
    logger.info(s"Beginning pipe with cmd $cmd")

    val quarantineInfo = quarantineLocation.map { a =>
      val (location, flavor) = a
      PipeIterator.QuarantineInfo(df, location, PipeIterator.QuarantineWriter(flavor))
    }
    val rawRdd = df.queryExecution.toRdd
    val inputRdd = if (rawRdd.getNumPartitions == 0) {
      logger.warn("Not piping any rows, as the input DataFrame has zero partitions.")
      SQLUtils.createEmptyRDD(df.sparkSession)
    } else {
      rawRdd
    }

    val errorPartitionData: Option[CollectionAccumulator[Any]] = Try {
      val ctx = inputRdd.context
      val acc = ctx.collectionAccumulator[Any]("errorPartitionData")
      acc
    } match {
      case Success(ful) => Some(ful)
      case Failure(t) =>
        t.printStackTrace()
        None
    }

    // Each partition consists of an iterator with the schema, followed by [[InternalRow]]s with the
    // schema
    val schemaInternalRowRDD = inputRdd.mapPartitions { it =>
      if (it.isEmpty) {
        Iterator.empty
      } else {
        new PipeIterator(cmd, env, it, informatter, outputformatter, errorPartitionData)
      }
    }.persist(StorageLevel.DISK_ONLY)

    cachedRdds.synchronized {
      cachedRdds.append(schemaInternalRowRDD)
    }

    // Quarantining is potentially very wasteful due to the throw-based control
    // flow implemented at the level below.
//    quarantineInfo.foreach { quarantineInfo =>
//      try {
//        schemaInternalRowRDD.mapPartitions { it =>
//          if (it.nonEmpty) {
//            val result = if (it.asInstanceOf[PipeIterator].error) {
//              Iterator(true)
//            } else {
//              Iterator.empty
//            }
//            result
//          } else {
//            Iterator.empty
//          }
//        }.filter(identity).take(1).nonEmpty
//      } catch { case _: Throwable => quarantineInfo.flavor.quarantine(quarantineInfo) }
//    }

    val schemaSeq = schemaInternalRowRDD.mapPartitions { it =>
      if (it.hasNext) {
        Iterator(it.next.asInstanceOf[StructType])
      } else {
        Iterator.empty
      }
    }.collect.distinct

    errorPartitionData.flatMap { errorPartitionData =>
      import collection.JavaConverters._
      val data = errorPartitionData.value.asScala
      if (data.nonEmpty) {
        Some(data)
      } else {
        None
      }
    } match {
      case None => // OK
        if (schemaSeq.length != 1) {
          throw new IllegalStateException(
            s"Cannot infer schema: saw ${schemaSeq.length} distinct schemas.")
        }

        val schema = schemaSeq.head
        val internalRowRDD = schemaInternalRowRDD.mapPartitions { it =>
          it.drop(1).asInstanceOf[Iterator[InternalRow]]
        }

        SQLUtils.internalCreateDataFrame(
          df.sparkSession,
          internalRowRDD,
          schema,
          isStreaming = false)
      case Some(errorPartitionData) =>
        val (data, exception) = errorPartitionData.partition {
          case a: InternalRow =>
            true
          case _ =>
            false
        }
        quarantineInfo.foreach { quarantineInfo =>
          val failedPartitions = data.map(_.asInstanceOf[InternalRow])
          val failedRDD = schemaInternalRowRDD.context.parallelize(failedPartitions)
          val failedDataframe =
            SQLUtils.internalCreateDataFrame(
              df.sparkSession,
              failedRDD,
              df.schema,
              isStreaming = false)
          quarantineInfo.flavor.quarantine(quarantineInfo.copy(df = failedDataframe))
        }

        val ex = exception.head.asInstanceOf[Throwable]
        throw new org.apache.spark.SparkException("Could not process data. " + ex.getMessage(), ex)
    }
  }
}

private[projectglow] class ProcessHelper(
    cmd: Seq[String],
    environment: Map[String, String],
    inputFn: OutputStream => Unit,
    context: TaskContext)
    extends GlowLogging {

  private val _childThreadException = new AtomicReference[Throwable](null)
  private var process: Process = _

  def startProcess(): BufferedInputStream = {
    val pb = new ProcessBuilder(cmd.asJava)
    val pbEnv = pb.environment()
    environment.foreach { case (k, v) => pbEnv.put(k, v) }
    process = pb.start()

    val stdinWriterThread = new Thread(s"${ProcessHelper.STDIN_WRITER_THREAD_PREFIX} for $cmd") {
      override def run(): Unit = {
        SQLUtils.setTaskContext(context)
        val out = process.getOutputStream
        try {
          inputFn(out)
        } catch {
          case t: Throwable => _childThreadException.set(t)
        } finally {
          out.close()
        }
      }
    }
    stdinWriterThread.start()

    val stderrReaderThread = new Thread(s"${ProcessHelper.STDERR_READER_THREAD_PREFIX} for $cmd") {
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
          case t: Throwable => _childThreadException.set(t)
        } finally {
          err.close()
        }
      }
    }
    stderrReaderThread.start()

    new BufferedInputStream(process.getInputStream)
  }

  def waitForProcess(): Int = {
    if (process == null) {
      throw new IllegalStateException(s"Process hasn't been started yet")
    }
    process.waitFor()
  }

  def propagateChildException(): Unit = {
    childThreadExceptionO.foreach { t =>
      processO.foreach(_.destroy())
      throw t
    }
  }

  def childThreadExceptionO: Option[Throwable] = Option(_childThreadException.get())
  def processO: Option[Process] = Option(process)
}

object ProcessHelper {
  val STDIN_WRITER_THREAD_PREFIX = "stdin writer"
  val STDERR_READER_THREAD_PREFIX = "stderr reader"
}

class PipeIterator(
    cmd: Seq[String],
    environment: Map[String, String],
    _input: Iterator[InternalRow],
    inputFormatter: InputFormatter[_],
    outputFormatter: OutputFormatter,
    errorPartitionData: Option[CollectionAccumulator[Any]] = None)
    extends Iterator[Any] {
  import PipeIterator.illegalStateException

  private val input = _input.toSeq
  private val processHelper = new ProcessHelper(cmd, environment, writeInput, TaskContext.get)
  private val inputStream = processHelper.startProcess()
  private val baseIterator = outputFormatter.makeIterator(inputStream)

  private def writeInput(stream: OutputStream): Unit = {
    WithUtils.withCloseable(inputFormatter) { informatter =>
      informatter.init(stream)
      input.foreach(informatter.write)
    }
  }

  override def hasNext: Boolean = {
    val result = if (baseIterator.hasNext) {
      true
    } else {
      val exitStatus = processHelper.waitForProcess()
      if (exitStatus != 0) {
        errorPartitionData.foreach { errorPartitionData =>
          input.foreach(errorPartitionData.add)
          errorPartitionData.add(
            illegalStateException(s"Subprocess exited with status $exitStatus"))
        }
      }
      false
    }
    processHelper.propagateChildException()
    result
  }

  override def next(): Any = baseIterator.next()

  def error: Boolean = {
    (0 == processHelper.waitForProcess())
  }
}

object PipeIterator {
  // This would typically be a typeclass, but this code base appears to be
  // subclass polymorphic rather than typeclass polymorphic
  trait QuarantineWriter extends Product with Serializable {
    def quarantine(qi: QuarantineInfo): Unit
  }
  final object QuarantineWriter {
    def apply(flavor: String): QuarantineWriter = flavor match {
      case "delta" => QuarantineWriterDelta
      case "csv" => QuarantineWriterCsv
      case _ => throw illegalStateException(s"unknown QuarantineWriter flavor: $flavor")
    }
  }
  final case object QuarantineWriterDelta extends QuarantineWriter {
    override def quarantine(qi: QuarantineInfo): Unit = {
      qi.df.write.format("delta").mode("append").saveAsTable(qi.location)
    }
  }
  final case object QuarantineWriterCsv extends QuarantineWriter {
    override def quarantine(qi: QuarantineInfo): Unit = {
      qi.df.write.mode("append").csv(qi.location)
    }
  }

  /* ~~~Scalastyle template evidently does not accept standard scaladoc comments~~~
   * ~~~Scalastyle states "Insert a space after the start of the comment"       ~~~
   * Data for Quarantining records which fail in process.
   * @param df The [[DataFrame]] being processed.
   * @param location The delta table to write to. Typically of the form `classifier.tableName`.
   */
  final case class QuarantineInfo(df: DataFrame, location: String, flavor: QuarantineWriter)

  def illegalStateException(message: String): IllegalStateException =
    new ISE("[PipeIterator] " + message)
}
