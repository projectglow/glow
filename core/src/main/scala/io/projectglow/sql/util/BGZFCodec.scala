package io.projectglow.sql.util

import java.io.OutputStream

import org.apache.hadoop.io.compress.{CompressionOutputStream, Compressor}
import org.seqdoop.hadoop_bam.util.{DatabricksBGZFOutputStream, BGZFCodec => HBBGZFCodec}

/**
 * A copy of Hadoop-BAM's BGZF codec that returns a Databricks BGZF output stream.
 */
class BGZFCodec extends HBBGZFCodec {
  override def createOutputStream(out: OutputStream): CompressionOutputStream = {
    new DatabricksBGZFOutputStream(out)
  }

  override def createOutputStream(
      out: OutputStream,
      compressor: Compressor): CompressionOutputStream = {
    createOutputStream(out)
  }
}
