package com.databricks.hls.transformers.pipe

import scala.collection.JavaConverters._

import java.io.InputStream

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A simple output formatter that returns each line of output as a String field.
 */
class UTF8TextOutputFormatter() extends OutputFormatter {

  override def makeIterator(stream: InputStream): Iterator[Any] = {
    val schema = StructType(Seq(StructField("text", StringType)))
    val iter = IOUtils.lineIterator(stream, "UTF-8").asScala.map { s =>
      new GenericInternalRow(Array(UTF8String.fromString(s)): Array[Any])
    }
    Iterator(schema) ++ iter
  }
}

class UTF8TextOutputFormatterFactory extends OutputFormatterFactory {
  override def name: String = "text"

  override def makeOutputFormatter(options: Map[String, String]): OutputFormatter = {
    new UTF8TextOutputFormatter
  }
}
