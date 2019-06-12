package org.seqdoop.hadoop_bam.util

import java.io.OutputStream

import htsjdk.samtools.util.{BlockCompressedOutputStream, BlockCompressedStreamConstants}
import org.apache.hadoop.io.compress.CompressionOutputStream

/**
 * A copy of Hadoop-BAM's [[BGZFCompressionOutputStream]] that allows us to set
 * whether an empty gzip block should be written at the end of the output stream.
 */
class DatabricksBGZFOutputStream(outputStream: OutputStream)
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

object DatabricksBGZFOutputStream {
  def setWriteEmptyBlockOnClose(os: OutputStream, value: Boolean): Unit = os match {
    case s: DatabricksBGZFOutputStream => s.writeEmptyBlockOnClose = value
    case _ => // No op
  }
}
