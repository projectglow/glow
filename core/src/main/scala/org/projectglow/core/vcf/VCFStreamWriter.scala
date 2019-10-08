package org.projectglow.core.vcf

import scala.collection.JavaConverters._
import java.io.{Closeable, OutputStream}

import htsjdk.variant.variantcontext.{GenotypeBuilder, VariantContext, VariantContextBuilder}
import htsjdk.variant.variantcontext.writer.{Options, VariantContextWriter, VariantContextWriterBuilder}
import htsjdk.variant.vcf.{VCFHeader, VCFHeaderLine}

/**
 * This internal row -> variant context stream writer maintains a header whose sample IDs are set
 * exactly once - priority is given to sample IDs provided in the constructor, with the first
 * written variant context used for sample inference if needed.
 *
 * Upon setting the header sample IDs, any empty sample IDs are replaced with defaults.
 *
 * If default header sample IDs were used, any written variant context's empty sample IDs are also
 * replaced with defaults.
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

  private var headerHasDefaultSampleIds = false
  private var headerHasSetSampleIds = false
  private var header: VCFHeader = providedSampleIds.map { sList =>
    headerHasSetSampleIds = true
    val nonMissingSamples = if (sList.exists(_.isEmpty)) {
      headerHasDefaultSampleIds = true
      VCFStreamWriter.replaceEmptySampleIds(sList)
    } else {
      sList
    }
    new VCFHeader(headerLineSet.asJava, nonMissingSamples.asJava)
  }.getOrElse(new VCFHeader(headerLineSet.asJava))

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

    val maybeReplacedVC = maybeReplaceDefaultSampleIds(vc)
    val vcBuilder = new VariantContextBuilder(maybeReplacedVC)
    val iterator = maybeReplacedVC.getAttributes().entrySet().iterator()
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
          "Missing samples for empty stream; header cannot be written properly.")
      }
      writer.writeHeader(header)
      headerHasBeenSetOrWritten = true
    }
    writer.close()
  }

  // Sets header with the sample IDs from a variant context (maintaining genotype order), replacing
  // any empty sample IDs.
  private def maybeSetHeaderSampleIds(vc: VariantContext): Unit = {
    if (!headerHasSetSampleIds) {
      if (vc.getSampleNamesOrderedByName.asScala.exists(_.isEmpty)) {
        val nonMissingSamples =
          VCFStreamWriter.replaceEmptySampleIds(vc.getGenotypes.asScala.map(_.getSampleName))
        headerHasDefaultSampleIds = true
        header = new VCFHeader(header.getMetaDataInInputOrder, nonMissingSamples.asJava)
      } else {
        header = new VCFHeader(
          header.getMetaDataInInputOrder,
          vc.getGenotypes.asScala.map(_.getSampleName).asJava)
      }
      headerHasSetSampleIds = true
    }
  }

  // Replaces any empty sample IDs in a variant context to be written if default sample IDs were
  // used to replace empty sample IDs in the header.
  private def maybeReplaceDefaultSampleIds(vc: VariantContext): VariantContext = {
    if (headerHasDefaultSampleIds) {
      var missingSampleIdx = 0
      val gtSeq = vc.getGenotypes.asScala.map { gt =>
        if (gt.getSampleName.isEmpty) {
          missingSampleIdx += 1
          val gtBuilder = new GenotypeBuilder(gt)
          gtBuilder.name(s"sample_$missingSampleIdx").make
        } else {
          gt
        }
      }
      val vcBuilder = new VariantContextBuilder(vc)
      vcBuilder.genotypes(gtSeq.asJava).make
    } else {
      vc
    }
  }
}

object VCFStreamWriter {
  // If any sample IDs are empty, replaces them with defaults.
  def replaceEmptySampleIds(sampleList: Seq[String]): Seq[String] = {
    var missingSampleIdx = 0
    sampleList.map { s =>
      if (s.isEmpty) {
        missingSampleIdx += 1
        s"sample_$missingSampleIdx"
      } else {
        s
      }
    }
  }
}
