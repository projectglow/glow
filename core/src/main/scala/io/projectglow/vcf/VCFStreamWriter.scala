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
import java.util.{ArrayList => JArrayList, HashSet => JHashSet}

import scala.collection.JavaConverters._

import htsjdk.variant.variantcontext.writer.{Options, VariantContextWriter, VariantContextWriterBuilder}
import htsjdk.variant.variantcontext.{Genotype, GenotypeBuilder, VariantContext, VariantContextBuilder}
import htsjdk.variant.vcf.{VCFHeader, VCFHeaderLine}

import io.projectglow.common.GlowLogging

/**
 * This internal row -> variant context stream writer maintains a header that is set exactly once. The sample IDs are
 * set by [[sampleIdInfo]] if predetermined, or inferred from the first written row otherwise.
 *
 * If missing sample IDs were used to set the header, sample IDs in all rows to be written will be replaced with those
 * from the header.
 * If all sample IDs are present when setting the header, sample IDs in written rows will not be replaced.
 * Mixed missing/present sample IDs are not permitted.
 *
 * @param stream The stream to write to
 * @param headerLineSet Header lines used to set the VCF header
 * @param sampleIdInfo Sample IDs, which may be predetermined or must be inferred
 * @param writeHeader Whether to write the header in this stream
 */
class VCFStreamWriter(
    stream: OutputStream,
    headerLineSet: Set[VCFHeaderLine],
    sampleIdInfo: SampleIdInfo,
    writeHeader: Boolean)
    extends Closeable
    with GlowLogging
    with Serializable {

  var header: VCFHeader = _
  var headerSampleSet: JHashSet[String] = _
  var replaceSampleIds: Boolean = _

  private val writer: VariantContextWriter = new VariantContextWriterBuilder()
    .clearOptions()
    .setOutputStream(stream)
    .setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
    .setOption(Options.WRITE_FULL_FORMAT_FIELD)
    .build

  def setHeader(vcBuilder: VariantContextBuilder): Unit = {
    val sampleIds = if (sampleIdInfo == InferSampleIds) {
      val vcSamples = vcBuilder.getGenotypes.asScala.map(_.getSampleName)
      val numTotalSamples = vcSamples.length
      val numPresentSamples = vcSamples.count(!_.isEmpty)

      if (numPresentSamples > 0) {
        if (numPresentSamples < numTotalSamples) {
          VCFWriterUtils.throwMixedSamplesFailure()
        }
        replaceSampleIds = false
        vcSamples.sorted
      } else {
        replaceSampleIds = true
        InferSampleIds.fromNumberMissing(numTotalSamples)
      }
    } else {
      replaceSampleIds = false
      sampleIdInfo.asInstanceOf[SampleIds].sortedSampleIds
    }
    val javaSampleIds = sampleIds.asJava
    headerSampleSet = new JHashSet(javaSampleIds)
    header = new VCFHeader(headerLineSet.asJava, javaSampleIds)
  }

  // Replace genotypes' missing sample IDs with those from the header
  def replaceMissingSampleIds(vcBuilder: VariantContextBuilder): VariantContextBuilder = {
    val oldGts = vcBuilder.getGenotypes
    val newGts = new JArrayList[Genotype](oldGts.size)
    var i = 0
    while (i < oldGts.size) {
      val oldGt = oldGts.get(i)
      val newGt = if (oldGt.getSampleName.isEmpty && i < header.getGenotypeSamples.size) {
        new GenotypeBuilder(oldGt).name(header.getGenotypeSamples.get(i)).make
      } else {
        oldGt
      }
      newGts.add(newGt)
      i += 1
    }
    vcBuilder.genotypes(newGts)
  }

  // Check that genotype sample IDs are the same as those in the header
  def checkInferredSampleIds(vcBuilder: VariantContextBuilder): VariantContextBuilder = {
    if (!vcBuilder.getGenotypes.getSampleNames.equals(headerSampleSet)) {
      VCFWriterUtils.throwSampleInferenceFailure()
    }
    vcBuilder
  }

  def write(vc: VariantContext): Unit = {
    val vcBuilder = VCFWriterUtils.convertVcAttributesToStrings(vc)

    if (header == null) {
      setHeader(vcBuilder)
      if (writeHeader) {
        writer.writeHeader(header)
      } else {
        writer.setHeader(header)
      }
    }

    val replacedVcBuilder = if (replaceSampleIds) {
      replaceMissingSampleIds(vcBuilder)
    } else {
      vcBuilder
    }

    val checkedVcBuilder = if (sampleIdInfo == InferSampleIds) {
      checkInferredSampleIds(replacedVcBuilder)
    } else {
      replacedVcBuilder
    }

    writer.add(checkedVcBuilder.make)
  }

  override def close(): Unit = {
    // Header must be written before closing writer, or else VCF readers will break.
    if (header == null && writeHeader) {
      if (sampleIdInfo == InferSampleIds) {
        throw new IllegalStateException(
          "Cannot infer header for empty partition; " +
          "we suggest calling coalesce or repartition to remove empty partitions.")
      }
      val sampleIds = sampleIdInfo.asInstanceOf[SampleIds].sortedSampleIds
      header = new VCFHeader(headerLineSet.asJava, sampleIds.asJava)
      writer.writeHeader(header)
    }

    try {
      writer.close()
    } catch {
      case e: Throwable =>
        logger.warn("Could not close writer: " + e.getMessage)
    }
  }
}
