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

package org.seqdoop.hadoop_bam.util

import java.io.OutputStream

import htsjdk.samtools.util.{BlockCompressedOutputStream, BlockCompressedStreamConstants}
import org.apache.hadoop.io.compress.CompressionOutputStream

/**
 * A copy of Hadoop-BAM's [[BGZFCompressionOutputStream]] that allows us to set
 * whether an empty gzip block should be written at the end of the output stream.
 */
class GlowBGZFOutputStream(outputStream: OutputStream)
    extends CompressionOutputStream(outputStream) {

  var writeEmptyBlockOnClose: Boolean = false
  private var haveWrittenEmptyBlock = false
  private var output = new BlockCompressedOutputStream(outputStream, null: java.io.File)

  override def write(b: Int): Unit = {
    output.write(b)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    output.write(b, off, len)
  }

  override def finish(): Unit = {
    output.flush()
  }

  override def resetState(): Unit = {
    output.flush()
    output = new BlockCompressedOutputStream(out, null: java.io.File)
  }

  override def close(): Unit = {
    output.flush() // don't close as we don't want to write terminator (empty gzip block)
    if (writeEmptyBlockOnClose && !haveWrittenEmptyBlock) {
      out.write(BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK)
      haveWrittenEmptyBlock = true
    }
    out.close()
  }
}

object GlowBGZFOutputStream {
  def setWriteEmptyBlockOnClose(os: OutputStream, value: Boolean): Unit = os match {
    case s: GlowBGZFOutputStream => s.writeEmptyBlockOnClose = value
    case _ => // No op
  }
}
