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

import scala.collection.JavaConverters._

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import io.projectglow.common.GlowLogging

class VCFFileWriter(
    headerLineSet: Set[VCFHeaderLine],
    sampleIdsMissingOpt: Option[(Seq[String], Boolean)],
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
  private var writer: VCFStreamWriter = if (sampleIdsMissingOpt.isDefined) {
    val (sampleIds, missing) = sampleIdsMissingOpt.get
    val header = new VCFHeader(headerLineSet.asJava, sampleIds.asJava)
    new VCFStreamWriter(stream, header, sampleIdsMissingOpt.isEmpty, missing, writeHeader)
  } else {
    null
  }

  override def write(row: InternalRow): Unit = {
    val vcOpt = converter.convert(row)
    if (vcOpt.isDefined) {
      val vc = vcOpt.get
      if (writer == null) {
        val vcSamples = vc.getGenotypes.asScala.map(_.getSampleName)
        val presentSamples = vcSamples.filterNot(VCFWriterUtils.sampleIsMissing)
        val numSamples = vcSamples.length

        val (sampleIds, missing) = presentSamples.length match {
          case 0 => (VCFWriterUtils.getMissingSampleIds(vcSamples.length), true)
          case `numSamples` => (vcSamples.sorted, false)
          case _ =>
            throw new IllegalArgumentException("Cannot mix missing and non-missing sample IDs.")
        }
        val header = new VCFHeader(headerLineSet.asJava, sampleIds.asJava)
        writer =
          new VCFStreamWriter(stream, header, sampleIdsMissingOpt.isEmpty, missing, writeHeader)
      }
      writer.write(vc)
    }
  }

  override def close(): Unit = {
    if (writer == null) {
      throw new IllegalStateException(
        "Cannot infer header for empty partition; " +
        "we suggest calling coalesce or repartition to remove empty partitions.")
    }
    writer.close()
  }
}
