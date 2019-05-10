package org.bdgenomics.adam.converters

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFHeaderLine
import org.bdgenomics.formats.avro.TranscriptEffect
import org.slf4j.Logger

object ADAMConverters {

  // Currently private in ADAM (TranscriptEffectConverter)
  def parseTranscriptEffect(s: String, stringency: ValidationStringency): Seq[TranscriptEffect] = {

    TranscriptEffectConverter.parseTranscriptEffect(s, stringency)
  }

  // Currently private in ADAM (VariantContextConverter)
  def cleanAndMixInSupportedLines(
      headerLines: Seq[VCFHeaderLine],
      stringency: ValidationStringency,
      log: Logger): Seq[VCFHeaderLine] = {

    VariantContextConverter.cleanAndMixInSupportedLines(headerLines, stringency, log)
  }
}
