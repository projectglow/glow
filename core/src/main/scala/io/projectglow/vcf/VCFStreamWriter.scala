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
import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConverters._

import htsjdk.variant.variantcontext.writer.{Options, VariantContextWriter, VariantContextWriterBuilder}
import htsjdk.variant.variantcontext.{Genotype, GenotypeBuilder, VariantContext, VariantContextBuilder}
import htsjdk.variant.vcf.{VCFHeader, VCFHeaderLine}

class VCFStreamWriter(
    stream: OutputStream,
    headerLineSet: Set[VCFHeaderLine],
    sampleIdsMissingOpt: Option[(Seq[String], Boolean)],
    writeHeader: Boolean)
    extends Closeable
    with Serializable {

  // Header should be set or written exactly once
  var headerHasBeenSetOrWritten = false

  val inferSampleIds: Boolean = sampleIdsMissingOpt.isEmpty
  var header: VCFHeader = _
  var replaceSampleIds: Boolean = _

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

    if (header == null) {
      val (sampleIds, missing) = if (!inferSampleIds) {
        sampleIdsMissingOpt.get
      } else {
        val vcSamples = vc.getGenotypes.asScala.map(_.getSampleName)
        val presentSamples = vcSamples.filterNot(VCFWriterUtils.sampleIsMissing)
        val numSamples = vcSamples.length
        presentSamples.length match {
          case 0 => (VCFWriterUtils.getMissingSampleIds(vcSamples.length), true)
          case `numSamples` => (vcSamples.sorted, false)
          case _ =>
            throw new IllegalArgumentException("Cannot mix missing and non-missing sample IDs.")
        }
      }
      header = new VCFHeader(headerLineSet.asJava, sampleIds.asJava)
      replaceSampleIds = missing
      if (writeHeader) {
        writer.writeHeader(header)
      } else {
        writer.setHeader(header)
      }
    }

    if (replaceSampleIds) {
      val oldGts = vcBuilder.getGenotypes
      if (inferSampleIds && oldGts.size != header.getNGenotypeSamples) {
        if (oldGts.asScala.map(_.getSampleName).exists(s => s != "")) {
          throw new IllegalArgumentException("Cannot mix missing and non-missing sample IDs.")
        }
        throw new IllegalArgumentException(
          "Number of genotypes in row does not match number of injected missing header samples.")
      }
      val newGts = new JArrayList[Genotype](oldGts.size)
      var i = 0
      while (i < oldGts.size) {
        val oldGt = oldGts.get(i)
        if (inferSampleIds && !oldGt.getSampleName.isEmpty) {
          throw new IllegalArgumentException("Cannot mix missing and non-missing sample IDs.")
        }
        val newGt = new GenotypeBuilder(oldGt).name(header.getGenotypeSamples.get(i)).make
        newGts.add(newGt)
        i += 1
      }
      vcBuilder.genotypes(newGts)
    } else if (inferSampleIds) {
      val vcSamples = vcBuilder.getGenotypes.asScala.map(_.getSampleName)
      var i = 0
      while (i < vcSamples.size) {
        val gtSample = vcSamples(i)
        if (gtSample == "") {
          throw new IllegalArgumentException("Cannot mix missing and non-missing sample IDs.")
        } else if (!header.getGenotypeSamples.contains(gtSample)) {
          throw new IllegalArgumentException(
            "Found sample ID in row that was not present in the header.")
        }
        i += 1
      }
    }

    writer.add(vcBuilder.make)

  }

  override def close(): Unit = {
    // Header must be written before closing writer, or else VCF readers will break.
    if (header == null && writeHeader) {
      if (inferSampleIds) {
        throw new IllegalStateException(
          "Cannot infer header for empty partition; " +
          "we suggest calling coalesce or repartition to remove empty partitions.")
      }
      val (sampleIds, _) = sampleIdsMissingOpt.get
      header = new VCFHeader(headerLineSet.asJava, sampleIds.asJava)
      writer.writeHeader(header)
      headerHasBeenSetOrWritten = true
    }
    writer.close()
  }
}
