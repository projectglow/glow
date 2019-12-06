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

  /**
   * Infer sample IDs from a genomic DataFrame.
   *
   * - If there are no genotypes, there are no sample IDs.
   * - If there are genotypes and sample IDs are all...
   *     - Missing, the sample IDs are injected from the number of genotypes per row (must be the same per row).
   *     - Present, the sample IDs are found by unifying sample IDs across all rows.
   */
  def inferSampleIdsAndInjectMissing(data: DataFrame): (Seq[String], Boolean) = {
    val genotypeSchemaOpt = data
      .schema
      .find(_.name == "genotypes")
      .map(_.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType])
    if (genotypeSchemaOpt.isEmpty) {
      logger.warn("No genotypes column, no sample IDs will be inferred.")
      return (Seq.empty, false)
    }
    val genotypeSchema = genotypeSchemaOpt.get

    import data.sparkSession.implicits._
    val hasSampleIdsColumn = genotypeSchema.exists(_.name == "sampleId")
    if (hasSampleIdsColumn) {
      val sampleLists = data
        .select("genotypes.sampleId")
        .distinct()
        .as[Array[String]]
        .collect
      val distinctSampleIds = sampleLists.flatten.distinct
      val presentSampleIds = distinctSampleIds.filterNot(sampleIsMissing)
      val numSampleIds = distinctSampleIds.length

      presentSampleIds.length match {
        case 0 => (injectMissingSampleIds(sampleLists.map(_.length)), true)
        case `numSampleIds` => (presentSampleIds.sorted, false)
        case _ =>
          throw new IllegalArgumentException("Cannot mix missing and non-missing sample IDs.")
      }
    } else {
      val numMissingSampleList = data
        .selectExpr("size(genotypes)")
        .distinct()
        .as[Int]
        .collect

      (injectMissingSampleIds(numMissingSampleList), true)
    }
  }

  def sampleIsMissing(s: String): Boolean = {
    s == null || s.isEmpty
  }

  def injectMissingSampleIds(missingSampleLists: Seq[Int]): Seq[String] = {
    logger.warn("Detected missing sample IDs, inferring sample IDs.")
    if (missingSampleLists.length > 1) {
      throw new IllegalArgumentException(
        "Rows contain varying number of missing samples; cannot infer sample IDs.")
    }
    getMissingSampleIds(missingSampleLists.head)
  }

  def getMissingSampleIds(numMissingSamples: Int): Seq[String] = {
    (1 to numMissingSamples).map { idx =>
      "sample_" + idx
    }
  }
}
