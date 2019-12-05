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
import java.util.{ArrayList => JArrayList, HashSet => JHashSet, Set => JSet}

import scala.collection.JavaConverters._
import htsjdk.variant.variantcontext.writer.{Options, VariantContextWriter, VariantContextWriterBuilder}
import htsjdk.variant.variantcontext.{Genotype, GenotypeBuilder, VariantContext, VariantContextBuilder}
import htsjdk.variant.vcf.{VCFHeader, VCFHeaderLine}

class VCFStreamWriter(
    stream: OutputStream,
    headerLineSet: Set[VCFHeaderLine],
    providedSampleIds: Option[Seq[String]],
    writeHeader: Boolean)
    extends Closeable
    with Serializable {

  // Header should be set or written exactly once
  var headerHasBeenSetOrWritten = false

  // Header variables must stay in sync
  private var headerHasReplacedMissingSamples: Boolean = false
  private var headerHasSetSamples: Boolean = false
  private var header: VCFHeader = new VCFHeader(headerLineSet.asJava)
  if (providedSampleIds.isDefined) {
    setHeaderSamples(providedSampleIds.get)
  }
  private var headerSampleSet: JSet[String] = new JHashSet[String](header.getGenotypeSamples)

  private val writer: VariantContextWriter = new VariantContextWriterBuilder()
    .clearOptions()
    .setOutputStream(stream)
    .setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
    .setOption(Options.WRITE_FULL_FORMAT_FIELD)
    .build

  def write(vc: VariantContext): Unit = {
    val vcBuilder = new VariantContextBuilder(vc)
    val iterator = vc.getAttributes.entrySet().iterator()
    while (iterator.hasNext) { // parse to string, then write,
      // otherwise the write messes up double precisions
      val entry = iterator.next()
      vcBuilder.attribute(
        entry.getKey,
        VariantContextToVCFRowConverter.parseObjectAsString(entry.getValue))
    }

    if (!headerHasBeenSetOrWritten) {
      if (!headerHasSetSamples) {
        setHeaderSamples(vc.getGenotypes.asScala.map(_.getSampleName))
      }
      if (writeHeader) {
        writer.writeHeader(header)
      } else {
        writer.setHeader(header)
      }
      headerHasBeenSetOrWritten = true
    }

    val vcBuilderNotMissingSamples = if (headerHasReplacedMissingSamples) {
      // Don't bother inferring missing sample IDs unless we did so for the header
      if (vcBuilder.getGenotypes.size != headerSampleSet.size) {
        throw new IllegalArgumentException(
          "Number of missing sample names does not match between VCF header and row to write.")
      }
      VCFStreamWriter.replaceMissingSampleIds(vcBuilder)
    } else {
      // Mismatched samples can only happen with the sharded VCF writer
      if (vcBuilder.getGenotypes.getSampleNames.asScala.exists(_.isEmpty)) {
        throw new IllegalArgumentException(
          "Inferred VCF header contains no missing sample names, but rows to write are missing sample names.")
      }
      vcBuilder
    }

    if (providedSampleIds.isEmpty && !headerSampleSet.containsAll(
        vcBuilderNotMissingSamples.getGenotypes.getSampleNames)) {
      // Mismatched samples can only happen with the sharded VCF writer
      throw new IllegalArgumentException(
        "Inferred VCF header is missing samples found in the data; please provide a complete header or VCF file path.")
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

  // Sets the header with sample IDs.
  private def setHeaderSamples(sList: Seq[String]): Unit = {
    header = if (sList.exists(_.isEmpty)) {
      headerHasReplacedMissingSamples = true
      new VCFHeader(
        header.getMetaDataInInputOrder,
        VCFStreamWriter.setReplacedMissingSampleIds(sList).asJava)
    } else {
      new VCFHeader(header.getMetaDataInInputOrder, sList.asJava)
    }
    headerSampleSet = new JHashSet[String](header.getGenotypeSamples)
    headerHasSetSamples = true
  }

}

object VCFStreamWriter {

  private def setReplacedMissingSampleIds(sampleList: Seq[String]): Seq[String] = {
    sampleList.indices.map { idx =>
      if (sampleList(idx) != "") {
        throw new IllegalArgumentException(
          "Cannot mix present and missing sample names when inferring header sample IDs.")
      }
      "sample_" + (idx + 1)
    }
  }

  private def replaceMissingSampleIds(vcBuilder: VariantContextBuilder): VariantContextBuilder = {
    val oldGts = vcBuilder.getGenotypes
    val numGts = oldGts.size()
    val newGts = new JArrayList[Genotype](numGts)
    var i = 0
    while (i < numGts) {
      val oldGt = oldGts.get(i)
      if (oldGt.getSampleName != "") {
        throw new IllegalArgumentException(
          "Header sample IDs were inferred from missing sample IDs, cannot mix present and missing sample IDs.")
      }
      val newGt = if (oldGt.getSampleName == "") {
        new GenotypeBuilder(oldGt).name("sample_" + (i + 1)).make
      } else {
        oldGt
      }
      newGts.add(newGt)
      i += 1
    }
    vcBuilder.genotypes(newGts)
  }
}
