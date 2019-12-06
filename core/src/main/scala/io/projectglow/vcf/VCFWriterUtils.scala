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
   *     - Missing, the sample IDs are injected from the number of genotypes per row (must be the same per row).
   *     - Present, the sample IDs are found by unifying sample IDs across all rows.
   */
  def inferSampleIdsAndInjectMissing(data: DataFrame): SampleIdsFromMissing = {
    val genotypeSchemaOpt = data
      .schema
      .find(_.name == "genotypes")
      .map(_.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])
    if (genotypeSchemaOpt.isEmpty) {
      logger.info("No genotypes column, no sample IDs will be inferred.")
      return SampleIdsFromMissing.noSamples
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
        return SampleIdsFromMissing.presentSamples(distinctSampleIds)
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
    SampleIdsFromMissing.missingSamples(numGenotypesPerRow.headOption.getOrElse(0))
  }

  def sampleIsMissing(s: String): Boolean = {
    s == null || s.isEmpty
  }
}

case class SampleIdsFromMissing(sampleIds: Seq[String], fromMissing: Boolean)

object SampleIdsFromMissing {
  val noSamples = SampleIdsFromMissing(Seq.empty, fromMissing = false)

  def presentSamples(presentSampleIds: Seq[String]): SampleIdsFromMissing = {
    SampleIdsFromMissing(presentSampleIds.sorted, fromMissing = false)
  }

  def missingSamples(numMissingSamples: Int): SampleIdsFromMissing = {
    val injectedMissingSamples = (1 to numMissingSamples).map { idx =>
      "sample_" + idx
    }
    SampleIdsFromMissing(injectedMissingSamples, true)
  }
}
