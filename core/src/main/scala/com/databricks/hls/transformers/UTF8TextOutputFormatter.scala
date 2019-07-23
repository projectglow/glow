package com.databricks.hls.transformers

import java.io.InputStream

import scala.collection.JavaConverters._

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A simple output formatter that returns each line of output as a String field.
 */
class UTF8TextOutputFormatter() extends OutputFormatter {
  override def outputSchema(stream: InputStream): StructType =
    StructType(
      Seq(
        StructField("text", StringType)
      ))

  override def makeIterator(schema: StructType, stream: InputStream): Iterator[InternalRow] = {
    IOUtils.lineIterator(stream, "UTF-8").asScala.map { s =>
      new GenericInternalRow(Array(UTF8String.fromString(s)): Array[Any])
    }
  }
}

class UTF8TextOutputFormatterFactory extends OutputFormatterFactory {
  override def name: String = "text"

  override def makeOutputFormatter(options: Map[String, String]): OutputFormatter = {
    new UTF8TextOutputFormatter
  }
}
