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

package io.projectglow.vcf

import java.io.InputStream

import htsjdk.samtools.ValidationStringency
import htsjdk.tribble.readers.{AsciiLineReader, AsciiLineReaderIterator}
import htsjdk.variant.vcf.{VCFCodec, VCFHeader}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection

import io.projectglow.common.GlowLogging
import io.projectglow.transformers.pipe.{OutputFormatter, OutputFormatterFactory}

class VCFOutputFormatter extends OutputFormatter with GlowLogging {
  override def makeIterator(stream: InputStream): Iterator[Any] = {
    val codec = new VCFCodec
    val lineIterator = new AsciiLineReaderIterator(AsciiLineReader.from(stream))
    if (!lineIterator.hasNext) {
      return Iterator.empty
    }
    val header = codec.readActualHeader(lineIterator).asInstanceOf[VCFHeader]
    val schema = VCFSchemaInferrer.inferSchema(true, true, header)
    val converter =
      new VariantContextToInternalRowConverter(header, schema, ValidationStringency.LENIENT)

    val internalRowIter: Iterator[InternalRow] = new Iterator[InternalRow] {
      private val projection = UnsafeProjection.create(schema)
      private var nextRecord: InternalRow = null
      private def readNextVc(): Unit = {
        while (nextRecord == null && lineIterator.hasNext) {
          val decoded = codec.decode(lineIterator.next())
          if (decoded != null) {
            nextRecord = converter.convertRow(decoded, isSplit = false).copy()
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
    Iterator(schema) ++ internalRowIter
  }
}

class VCFOutputFormatterFactory extends OutputFormatterFactory {
  override def name: String = "vcf"

  override def makeOutputFormatter(options: Map[String, String]): OutputFormatter = {
    new VCFOutputFormatter()
  }
}
