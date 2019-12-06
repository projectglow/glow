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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import htsjdk.variant.vcf.VCFHeader

import io.projectglow.common.GlowLogging
import io.projectglow.transformers.pipe.{InputFormatter, InputFormatterFactory}

/**
 * An input formatter that writes rows as VCF records.
 */
class VCFInputFormatter(
    converter: InternalRowToVariantContextConverter,
    sampleIds: Seq[String],
    injectedMissing: Boolean)
    extends InputFormatter
    with GlowLogging {

  private var writer: VCFStreamWriter = _
  private var stream: OutputStream = _

  override def init(stream: OutputStream): Unit = {
    this.stream = stream
    val header = new VCFHeader(converter.vcfHeader.getMetaDataInInputOrder, sampleIds.asJava)
    this.writer = new VCFStreamWriter(stream, header, false, injectedMissing, writeHeader = true)
  }

  override def write(record: InternalRow): Unit = {
    converter.convert(record).foreach(writer.write)
  }

  override def close(): Unit = {
    logger.info("Closing VCF input formatter")
    writer.close()
  }
}

class VCFInputFormatterFactory extends InputFormatterFactory {
  override def name: String = "vcf"

  override def makeInputFormatter(df: DataFrame, options: Map[String, String]): InputFormatter = {
    val (headerLineSet, providedSampleIdsOpt) =
      VCFHeaderUtils.parseHeaderLinesAndSamples(
        options,
        None,
        df.schema,
        df.sparkSession.sparkContext.hadoopConfiguration)
    val rowConverter = new InternalRowToVariantContextConverter(
      df.schema,
      headerLineSet,
      VCFOptionParser.getValidationStringency(options)
    )
    rowConverter.validate()

    val (sampleIds, injectedMissing) = if (providedSampleIdsOpt.isDefined) {
      (providedSampleIdsOpt.get, false)
    } else {
      VCFWriterUtils.inferSampleIdsAndInjectMissing(df)
    }

    new VCFInputFormatter(rowConverter, sampleIds, injectedMissing)
  }
}
