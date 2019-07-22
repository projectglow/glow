package com.databricks.vcf

import java.io.InputStream

import htsjdk.samtools.ValidationStringency
import htsjdk.tribble.readers.{AsciiLineReader, AsciiLineReaderIterator}
import htsjdk.variant.vcf.{VCFCodec, VCFHeader}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import com.databricks.hls.common.HLSLogging
import com.databricks.hls.transformers.{OutputFormatter, OutputFormatterFactory}

class VCFOutputFormatter extends OutputFormatter with HLSLogging {
  override def makeIterator(schema: StructType, stream: InputStream): Iterator[InternalRow] = {
    val codec = new VCFCodec
    val lineIterator = new AsciiLineReaderIterator(AsciiLineReader.from(stream))
    if (!lineIterator.hasNext) {
      return Iterator.empty
    }
    val header = codec.readActualHeader(lineIterator).asInstanceOf[VCFHeader]
    val schemaFromHeader = VCFSchemaInferer.inferSchema(true, true, header)
    require(
      schemaFromHeader == schema,
      s"Data schema did not match provided schema. " +
      s"Data schema: $schemaFromHeader Provided schema: $schema")
    val converter =
      new VariantContextToInternalRowConverter(header, schema, ValidationStringency.LENIENT)

    new Iterator[InternalRow] {
      private var nextRecord: InternalRow = null
      private def readNextVc(): Unit = {
        while (nextRecord == null && lineIterator.hasNext) {
          val decoded = codec.decode(lineIterator.next())
          if (decoded != null) {
            nextRecord = converter.convertRow(decoded, isSplit = false)
          }
        }
      }

      override def hasNext: Boolean = {
        readNextVc()
        nextRecord != null
      }

      override def next(): InternalRow = {
        readNextVc()
        if (nextRecord != null) {
          val ret = nextRecord
          nextRecord = null
          ret
        } else {
          throw new NoSuchElementException("Iterator is empty")
        }
      }
    }
  }

  override def outputSchema(stream: InputStream): StructType = {
    val codec = new VCFCodec
    val lineIterator = new AsciiLineReaderIterator(AsciiLineReader.from(stream))
    if (!lineIterator.hasNext) {
      throw new IllegalStateException("Output VCF schema cannot be determined without a header.")
    }
    val header = codec.readActualHeader(lineIterator).asInstanceOf[VCFHeader]
    VCFSchemaInferer.inferSchema(true, true, header)
  }
}

class VCFOutputFormatterFactory extends OutputFormatterFactory {
  override def name: String = "vcf"

  override def makeOutputFormatter(options: Map[String, String]): OutputFormatter = {
    new VCFOutputFormatter()
  }
}
