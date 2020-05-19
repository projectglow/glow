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

import htsjdk.variant.variantcontext.{VariantContext, VariantContextBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ArrayType, StructType}

import io.projectglow.common.GlowLogging

object VCFWriterUtils extends GlowLogging {

  def throwMixedSamplesFailure(): Unit = {
    throw new IllegalArgumentException("Cannot mix missing and non-missing sample IDs.")
  }

  def throwSampleInferenceFailure(): Unit = {
    throw new IllegalArgumentException(
      "Cannot infer sample ids because they are not the same in every row.")
  }

  /**
   * Infer sample IDs from a genomic DataFrame.
   *
   * - If there are no genotypes, there are no sample IDs.
   * - If there are genotypes and sample IDs are all...
   *   - Present, the sample IDs are found by unifying sample IDs across all rows.
   *   - Missing, checks that the rows have the same number of genotypes.
   */
  def inferSampleIdsIfPresent(data: DataFrame): SampleIdInfo = {
    val genotypeSchemaOpt = data
      .schema
      .find(_.name == "genotypes")
      .map(_.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])
    if (genotypeSchemaOpt.isEmpty) {
      logger.info("No genotypes column, no sample IDs will be inferred.")
      return SampleIds(Seq.empty)
    }
    val genotypeSchema = genotypeSchemaOpt.get

    import data.sparkSession.implicits._
    val hasSampleIdsColumn = genotypeSchema.exists(_.name == "sampleId")

    if (hasSampleIdsColumn) {
      val distinctSampleIds = data
        .selectExpr("explode(genotypes.sampleId)")
        .distinct()
        .as[String]
        .collect
      val numPresentSampleIds = distinctSampleIds.count(!sampleIsMissing(_))

      if (numPresentSampleIds > 0) {
        if (numPresentSampleIds < distinctSampleIds.length) {
          throwMixedSamplesFailure()
        }
        return SampleIds(distinctSampleIds)
      }
    }

    val numGenotypesPerRow = data
      .selectExpr("size(genotypes)")
      .distinct()
      .as[Int]
      .collect

    if (numGenotypesPerRow.length > 1) {
      throw new IllegalArgumentException(
        "Rows contain varying number of missing samples; cannot infer sample IDs.")
    }

    logger.warn("Detected missing sample IDs, inferring sample IDs.")
    InferSampleIds
  }

  def sampleIsMissing(s: String): Boolean = {
    s == null || s.isEmpty
  }

  def convertVcAttributesToStrings(vc: VariantContext): VariantContextBuilder = {
    val vcBuilder = new VariantContextBuilder(vc)
    val iterator = vc.getAttributes.entrySet().iterator()
    while (iterator.hasNext) {
      // parse to string, then write, as the VCF encoder messes up double precisions
      val entry = iterator.next()
      vcBuilder.attribute(
        entry.getKey,
        VariantContextToInternalRowConverter.parseObjectAsString(entry.getValue))
    }
    vcBuilder
  }
}

case class SampleIds(unsortedSampleIds: Seq[String]) extends SampleIdInfo {
  val sortedSampleIds: Seq[String] = unsortedSampleIds.sorted
}
case object InferSampleIds extends SampleIdInfo {
  def fromNumberMissing(numMissingSamples: Int): Seq[String] = {
    (1 to numMissingSamples).map { idx =>
      "sample_" + idx
    }
  }
}

sealed trait SampleIdInfo
