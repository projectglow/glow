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

import java.io.OutputStream

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType

import io.projectglow.common.GlowLogging

class VCFFileWriter(
    val path: String,
    headerLineSet: Set[VCFHeaderLine],
    sampleIdInfo: SampleIdInfo,
    stringency: ValidationStringency,
    schema: StructType,
    conf: Configuration,
    stream: OutputStream,
    writeHeader: Boolean)
    extends OutputWriter
    with GlowLogging {

  private val converter =
    new InternalRowToVariantContextConverter(schema, headerLineSet, stringency)
  converter.validate()
  private val writer: VCFStreamWriter =
    new VCFStreamWriter(stream, headerLineSet, sampleIdInfo, writeHeader)

  override def write(row: InternalRow): Unit = {
    converter.convert(row).foreach(writer.write)
  }

  override def close(): Unit = {
    writer.close()
  }
}
