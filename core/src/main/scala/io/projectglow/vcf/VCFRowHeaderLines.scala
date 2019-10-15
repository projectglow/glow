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

import htsjdk.variant.vcf._

// Default set of VCF header lines parsed in VCFRow.
object VCFRowHeaderLines {

  lazy val end = VCFStandardHeaderLines.getInfoLine(VCFConstants.END_KEY)

  lazy val genotype = VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_KEY)
  lazy val depth = VCFStandardHeaderLines.getFormatLine(VCFConstants.DEPTH_KEY)
  lazy val filters = VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_FILTER_KEY)
  lazy val genotypeLikelihoods = new VCFFormatHeaderLine(
    VCFConstants.GENOTYPE_LIKELIHOODS_KEY,
    VCFHeaderLineCount.G,
    VCFHeaderLineType.Float,
    "Genotype likelihoods"
  )
  lazy val phredLikelihoods = VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_PL_KEY)
  lazy val posteriorProbabilities = new VCFFormatHeaderLine(
    VCFConstants.GENOTYPE_POSTERIORS_KEY,
    VCFHeaderLineCount.G,
    VCFHeaderLineType.Float,
    "Genotype posterior probabilities"
  )
  lazy val conditionalQuality =
    VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_QUALITY_KEY)
  lazy val haplotypeQualities = new VCFFormatHeaderLine(
    VCFConstants.HAPLOTYPE_QUALITY_KEY,
    2,
    VCFHeaderLineType.Integer,
    "Haplotype quality"
  )
  lazy val expectedAlleleCounts = new VCFFormatHeaderLine(
    VCFConstants.EXPECTED_ALLELE_COUNT_KEY,
    VCFHeaderLineCount.A,
    VCFHeaderLineType.Integer,
    "Expected alternate allele counts"
  )
  lazy val mappingQuality = new VCFFormatHeaderLine(
    VCFConstants.RMS_MAPPING_QUALITY_KEY,
    1,
    VCFHeaderLineType.Float,
    "Root mean square (RMS) mapping quality"
  )
  lazy val alleleDepths = VCFStandardHeaderLines.getFormatLine(VCFConstants.GENOTYPE_ALLELE_DEPTHS)

  lazy val infoHeaderLines: Seq[VCFInfoHeaderLine] = Seq(end)

  // Default set of VCF header lines for format fields parsed in VCFRow.
  lazy val formatHeaderLines: Seq[VCFFormatHeaderLine] = Seq(
    genotype,
    depth,
    filters,
    genotypeLikelihoods,
    phredLikelihoods,
    posteriorProbabilities,
    conditionalQuality,
    haplotypeQualities,
    expectedAlleleCounts,
    mappingQuality,
    alleleDepths
  )

  lazy val allHeaderLines: Seq[VCFHeaderLine] = infoHeaderLines ++ formatHeaderLines
}
