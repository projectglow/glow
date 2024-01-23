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

package io.projectglow.sql.util

import java.io.Closeable
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, LineRecordReader}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.sql.execution.datasources.RecordReaderIterator

import io.projectglow.common.GlowLogging

/**
 * Identical to [[org.apache.spark.sql.execution.datasources.HadoopFileLinesReader]], but
 * takes the individual fields of [[org.apache.spark.sql.execution.datasources.PartitionedFile]]
 * instead of the object itself. PartitionedFile objects cannot be instantiated in DBR
 * because of a binary incompatible change vs OSS Spark.
 */
class HadoopLineIterator(
    path: String,
    start: Long,
    length: Long,
    lineSeparator: Option[Array[Byte]],
    conf: Configuration)
    extends Iterator[Text]
    with Closeable
    with GlowLogging {

  private val it = {
    val fileSplit = new FileSplit(
      new Path(new URI(path)),
      start,
      length,
      // TODO: Implement Locality
      Array.empty
    )
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

    val reader = lineSeparator match {
      case Some(sep) => new LineRecordReader(sep)
      // If the line separator is `None`, it covers `\r`, `\r\n` and `\n`.
      case _ => new LineRecordReader()
    }

    reader.initialize(fileSplit, hadoopAttemptContext)
    new RecordReaderIterator(reader)
  }

  override def hasNext: Boolean = {
    it.hasNext
  }

  override def next(): Text = {
    it.next()
  }

  override def close(): Unit = {
    it.close()
  }
}
