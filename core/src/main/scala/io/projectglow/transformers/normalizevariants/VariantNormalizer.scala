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

package io.projectglow.transformers.normalizevariants

import htsjdk.samtools.reference.ReferenceSequenceFile
import io.projectglow.common.GlowLogging
import io.projectglow.common.VariantSchemas._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object VariantNormalizer extends GlowLogging {

  /**
   * Contains the main normalization logic. Given contigName, start, end, refAllele, and
   * altAlleles of a variant as well as the indexed fasta file of the reference genome,
   * creates an InternalRow of the normalization result.
   *
   * The algorithm has a logic similar to bcftools norm or vt normalize:
   *
   * It starts from the rightmost base of all alleles and scans one base at a time incrementing
   * trimSize and nTrimmedBasesBeforeNextPadding as long as the bases of all alleles at that
   * position are the same. If the beginning of any of the alleles is reached, all alleles are
   * padded on the left by PAD_WINDOW_SIZE bases by reading from the reference genome and
   * nTrimmedBaseBeforeNextPadding is reset. The process continues until a position is reached
   * where all alleles do not have the same base or the beginning of the contig is reached. Next
   * trimming from left starts and all bases common among all alleles from left are trimmed.
   * Start and end are adjusted accordingly during the process.
   *
   * @param contigName            : Contig name of the alleles
   * @param start                 : 0-based start of the REF allele in an open-left closed-right interval system
   * @param end                   : 0-based end of the REF allele in an open-left closed-right interval system
   * @param refAllele             : String containing refrence allele
   * @param altAlleles            : String array of alternate alleles
   * @param refGenomeIndexedFasta : a [[ReferenceSequenceFile]] of the reference genome.
   * @return normalization result as an InternalRow
   */
  def normalizeVariant(
      contigName: String,
      start: Long,
      end: Long,
      refAllele: String,
      altAlleles: Array[String],
      refGenomeIndexedFasta: ReferenceSequenceFile): InternalRow = {

    var flag = false // indicates whether the variant was changed as a result of normalization
    var errorMessage: Option[String] = None
    var newStart = start
    var allAlleles = refAllele +: altAlleles
    var trimSize = 0 // stores total trimSize from right

    if (refAllele.isEmpty) {
      // if no alleles, throw exception
      logger.info("No REF or ALT alleles found.")
      errorMessage = Some("No REF or ALT alleles found.")
    } else if (altAlleles.isEmpty) {
      // if only one allele and longer than one base, trim to the
      // first base
      allAlleles(0) = allAlleles(0).take(1)
      flag = true
    } else if (!isSNP(refAllele, altAlleles) || !isSymbolic(altAlleles)) {

      // Trim from right
      var nTrimmedBasesBeforeNextPadding = 0 // stores number of bases trimmed from right before next padding
      var firstBaseFromRightInRefAllele =
        allAlleles(0)(allAlleles(0).length - nTrimmedBasesBeforeNextPadding - 1)

      while (allAlleles
          .forall(a =>
            a(a.length - nTrimmedBasesBeforeNextPadding - 1) == firstBaseFromRightInRefAllele)) {
        // Last base in all alleles are the same

        var padSeq = ""
        var nPadBases = 0

        if (allAlleles
            .map(_.length)
            .min == nTrimmedBasesBeforeNextPadding + 1) {
          // if beginning of any allele is reached, trim from right what
          // needs to be trimmed so far, and pad to the left
          if (newStart > 0) {
            nPadBases = if (PAD_WINDOW_SIZE <= newStart) {
              PAD_WINDOW_SIZE
            } else {
              newStart.toInt
            }

            padSeq ++= refGenomeIndexedFasta
              .getSubsequenceAt(contigName, newStart - nPadBases + 1, newStart)
              .getBaseString()

          } else {
            nTrimmedBasesBeforeNextPadding -= 1
          }

          allAlleles = allAlleles.map { a =>
            padSeq ++ a.dropRight(nTrimmedBasesBeforeNextPadding + 1)
          }

          trimSize += nTrimmedBasesBeforeNextPadding + 1

          newStart -= nPadBases

          nTrimmedBasesBeforeNextPadding = 0

        } else {

          nTrimmedBasesBeforeNextPadding += 1

        }

        firstBaseFromRightInRefAllele = allAlleles(0)(
          allAlleles(0).length - nTrimmedBasesBeforeNextPadding - 1
        )
      }

      // trim from left
      var nLeftTrimBases = 0
      var firstBaseFromLeftInRefAllele = allAlleles(0)(nLeftTrimBases)
      val minAlleleLength = allAlleles.map(_.length).min

      while (nLeftTrimBases < minAlleleLength - nTrimmedBasesBeforeNextPadding - 1
        && allAlleles.forall(_(nLeftTrimBases) == firstBaseFromLeftInRefAllele)) {

        nLeftTrimBases += 1

        firstBaseFromLeftInRefAllele = allAlleles(0)(nLeftTrimBases)
      }

      allAlleles = allAlleles.map { a =>
        a.drop(nLeftTrimBases)
          .dropRight(nTrimmedBasesBeforeNextPadding)
      }

      trimSize += nTrimmedBasesBeforeNextPadding

      newStart += nLeftTrimBases

      if (trimSize != 0 || nLeftTrimBases != 0) {
        flag = true
      }
    }

    val outputRow = new GenericInternalRow(5)

    if (errorMessage.isEmpty) {
      outputRow.update(0, newStart)
      outputRow.update(1, end - trimSize)
      outputRow.update(2, UTF8String.fromString(allAlleles(0)))
      outputRow.update(3, ArrayData.toArrayData(allAlleles.tail.map(UTF8String.fromString(_))))
    }

    outputRow.update(
      4,
      InternalRow(
        flag,
        errorMessage.map(UTF8String.fromString).orNull
      )
    )

    outputRow

  }

  def isSNP(refAllele: String, altAlleles: Array[String]): Boolean = {
    refAllele.length == 1 && altAlleles.forall(_.length == 1)
  }

  def isSymbolic(altAlleles: Array[String]): Boolean = {
    altAlleles.exists(_.matches(".*[<|>|*].*"))
  }

  private val PAD_WINDOW_SIZE = 100

  val normalizationResultFieldName = "normalizationResult"
  val normalizationStatusFieldName = "normalizationStatus"
  val changedFieldName = "changed"
  val errorMessageFieldName = "errorMessage"

  val normalizationStatusStructField = StructField(
    normalizationStatusFieldName,
    StructType(
      Seq(
        StructField(changedFieldName, BooleanType),
        StructField(errorMessageFieldName, StringType)
      )
    )
  )

  val normalizationResultStructType =
    StructType(
      Seq(
        startField,
        endField,
        refAlleleField,
        alternateAllelesField,
        normalizationStatusStructField
      )
    )

}
