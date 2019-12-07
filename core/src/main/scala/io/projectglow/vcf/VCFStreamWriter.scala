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

/**
 * This internal row -> variant context stream writer maintains a header that is set exactly once. The sample IDs are
 * set by [[sampleIdInfoOpt]] if provided, or inferred from the first written row otherwise.
 *
 * If missing sample IDs were used to set the header, sample IDs in all rows to be written will be replaced with those
 * from the header.
 * If all sample IDs are present when setting the header, sample IDs in written rows will not be replaced.
 * Mixed missing/present sample IDs are not permitted.
 *
 *
 * @param stream The stream to write to
 * @param headerLineSet Header lines used to set the VCF header
 * @param sampleIdInfoOpt Sample IDs (and if any were missing) if pre-determined, None otherwise
 * @param writeHeader Whether to write the header in this stream
 */
class VCFStreamWriter(
    stream: OutputStream,
    headerLineSet: Set[VCFHeaderLine],
    sampleIdInfoOpt: Option[SampleIdInfo],
    writeHeader: Boolean)
    extends Closeable
    with Serializable {

  // Header should be set or written exactly once
  var headerHasBeenSetOrWritten = false

  val inferSampleIds: Boolean = sampleIdInfoOpt.isEmpty
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
    val sampleIdInfo = if (!inferSampleIds) {
      sampleIdInfoOpt.get
    } else {
      val vcSamples = vcBuilder.getGenotypes.asScala.map(_.getSampleName)
      val numTotalSamples = vcSamples.length
      val numPresentSamples = vcSamples.count(!_.isEmpty)

      if (numPresentSamples > 0) {
        if (numPresentSamples < numTotalSamples) {
          VCFWriterUtils.throwMixedSamplesFailure()
        }
        SampleIdInfo.fromSamples(vcSamples)
      } else {
        SampleIdInfo.fromNumberMissing(numTotalSamples)
      }
    }
    val sampleIds = sampleIdInfo.sampleIds.asJava
    headerSampleSet = new JHashSet(sampleIds)
    header = new VCFHeader(headerLineSet.asJava, sampleIds)
    replaceSampleIds = sampleIdInfo.fromMissing
  }

  // Replace genotypes' missing sample IDs with those from the header
  def replaceMissingSampleIds(vcBuilder: VariantContextBuilder): VariantContextBuilder = {
    val oldGts = vcBuilder.getGenotypes
    if (inferSampleIds && oldGts.size != header.getNGenotypeSamples) {
      VCFWriterUtils.throwSampleInferenceFailure()
    }
    val newGts = new JArrayList[Genotype](oldGts.size)
    var i = 0
    while (i < oldGts.size) {
      val oldGt = oldGts.get(i)
      if (inferSampleIds && !oldGt.getSampleName.isEmpty) {
        VCFWriterUtils.throwSampleInferenceFailure()
      }
      val newGt = new GenotypeBuilder(oldGt).name(header.getGenotypeSamples.get(i)).make
      newGts.add(newGt)
      i += 1
    }
    vcBuilder.genotypes(newGts)
  }

  // Check that genotype sample IDs are a subset of those from the header
  def checkInferredSampleIds(vcBuilder: VariantContextBuilder): VariantContextBuilder = {
    vcBuilder.getGenotypes.asScala.foreach { gt =>
      if (!headerSampleSet.contains(gt.getSampleName)) {
        VCFWriterUtils.throwSampleInferenceFailure()
      }
    }
    vcBuilder
  }

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

    if (header == null) {
      setHeader(vcBuilder)
      if (writeHeader) {
        writer.writeHeader(header)
      } else {
        writer.setHeader(header)
      }
    }

    val checkedVcBuilder = if (replaceSampleIds) {
      replaceMissingSampleIds(vcBuilder)
    } else if (inferSampleIds) {
      checkInferredSampleIds(vcBuilder)
    } else {
      vcBuilder
    }

    writer.add(checkedVcBuilder.make)

  }

  override def close(): Unit = {
    // Header must be written before closing writer, or else VCF readers will break.
    if (header == null && writeHeader) {
      if (inferSampleIds) {
        throw new IllegalStateException(
          "Cannot infer header for empty partition; " +
          "we suggest calling coalesce or repartition to remove empty partitions.")
      }
      header = new VCFHeader(headerLineSet.asJava, sampleIdInfoOpt.get.sampleIds.asJava)
      writer.writeHeader(header)
      headerHasBeenSetOrWritten = true
    }
    writer.close()
  }
}
