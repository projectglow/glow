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

import java.io.{Closeable, OutputStream}
import java.util.{HashSet => JHashSet, Set => JSet}

import scala.collection.JavaConverters._

import htsjdk.variant.variantcontext.writer.{Options, VariantContextWriter, VariantContextWriterBuilder}
import htsjdk.variant.variantcontext.{VariantContext, VariantContextBuilder}
import htsjdk.variant.vcf.{VCFHeader, VCFHeaderLine}

/**
 * This internal row -> variant context stream writer maintains a header whose sample IDs are set
 * exactly once - priority is given to sample IDs provided in the constructor, with the first
 * written variant context used for sample inference if needed.
 */
class VCFStreamWriter(
    stream: OutputStream,
    headerLineSet: Set[VCFHeaderLine],
    providedSampleIds: Option[Seq[String]],
    writeHeader: Boolean)
    extends Closeable
    with Serializable {

  // Header should be set or written exactly once
  var headerHasBeenSetOrWritten = false

  private var headerHasSetSampleIds = providedSampleIds.isDefined
  private var header: VCFHeader = providedSampleIds.map { sList =>
    new VCFHeader(headerLineSet.asJava, sList.asJava)
  }.getOrElse(new VCFHeader(headerLineSet.asJava))
  private var sampleSet: JSet[String] = new JHashSet(header.getGenotypeSamples)

  private val writer: VariantContextWriter = new VariantContextWriterBuilder()
    .clearOptions()
    .setOutputStream(stream)
    .setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
    .setOption(Options.WRITE_FULL_FORMAT_FIELD)
    .build

  def write(vc: VariantContext): Unit = {
    if (!headerHasBeenSetOrWritten) {
      maybeSetHeaderSampleIds(vc)
      if (writeHeader) {
        writer.writeHeader(header)
      } else {
        writer.setHeader(header)
      }
      headerHasBeenSetOrWritten = true
    }

    if (providedSampleIds.isEmpty && !sampleSet.containsAll(vc.getSampleNames)) {
      throw new IllegalArgumentException(
        "Inferred VCF header is missing samples found in the data; please provide a complete header or VCF file path.")
    }

    val vcBuilder = new VariantContextBuilder(vc)
    val iterator = vc.getAttributes.entrySet().iterator()
    while (iterator.hasNext) { // parse to string, then write,
      // otherwise the write messes up double precisions
      val entry = iterator.next()
      vcBuilder.attribute(
        entry.getKey,
        VariantContextToVCFRowConverter.parseObjectAsString(entry.getValue))
    }

    writer.add(vcBuilder.make)

  }

  override def close(): Unit = {
    // Header must be written before closing writer, or else VCF readers will break.
    if (!headerHasBeenSetOrWritten && writeHeader) {
      if (providedSampleIds.isEmpty) {
        throw new IllegalStateException(
          "Cannot infer header for empty partition; " +
          "we suggest calling coalesce or repartition to remove empty partitions.")
      }
      writer.writeHeader(header)
      headerHasBeenSetOrWritten = true
    }
    writer.close()
  }

  // Sets header with the sample IDs from a variant context (maintaining genotype order).
  private def maybeSetHeaderSampleIds(vc: VariantContext): Unit = {
    if (!headerHasSetSampleIds) {
      sampleSet = vc.getSampleNames
      val sampleList = vc.getGenotypes.asScala.map(_.getSampleName).asJava
      header = new VCFHeader(header.getMetaDataInInputOrder, sampleList)
      headerHasSetSampleIds = true
    }
  }
}
