package io.projectglow.transformers.normalizevariants

import htsjdk.samtools.reference.IndexedFastaSequenceFile
import io.projectglow.common.GlowLogging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object VariantNormalizer extends GlowLogging {

  /**
   * Contains the main normalization logic. Given contigName, start, end, refAllele, and
   * altAlleles of a variant as well as the indexed fasta file of the reference genome,
   * creates an InternalRow of the normalized variant with the [[normalizationSchema]].
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
   * @param refGenomeIndexedFasta : an [[IndexedFastaSequenceFile]] of the reference genome.
   * @return normalized variant as an InternalRow
   */


  def normalizeVariant(contigName: String,
                        start: Long,
                        end: Long,
                        refAllele: String,
                        altAlleles: Array[String],
                        refGenomeIndexedFasta: IndexedFastaSequenceFile): InternalRow = {



    var flag = FLAG_UNCHANGED
    var newStart = start
    var allAlleles = refAllele +: altAlleles
    var trimSize = 0 // stores total trimSize from right

    if (refAllele.isEmpty) {
      // if no alleles, throw exception
      logger.info("No REF and ALT alleles...")
      flag = FLAG_ERROR
    } else if (refAllele.length == 1 && altAlleles.forall(_.length == 1)) {
      // if a SNP, do nothing
      flag = FLAG_UNCHANGED
    } else if (altAlleles.exists(_.matches(".*[<|>|*].*"))) {
      // if any of the alleles is symbolic, do nothing
      flag = FLAG_UNCHANGED
    } else if (altAlleles.isEmpty) {
      // if only one allele and longer than one base, trim to the
      // first base
      allAlleles(0) = allAlleles(0).take(1)
      flag = FLAG_CHANGED
    } else {

      // Trim from right
      var nTrimmedBasesBeforeNextPadding = 0 // stores number of bases trimmed from right before next padding
      var firstBaseFromRightInRefAllele = allAlleles(0)(
        allAlleles(0).length - nTrimmedBasesBeforeNextPadding - 1)


      while (allAlleles
          .forall(a => a(a.length - nTrimmedBasesBeforeNextPadding - 1) == firstBaseFromRightInRefAllele)) {
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

            padSeq ++= refGenomeIndexedFasta.getSubsequenceAt(contigName, newStart - nPadBases + 1, newStart).getBaseString()

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

      flag = if (trimSize == 0 && nLeftTrimBases == 0) {
        FLAG_UNCHANGED
      } else {
        FLAG_CHANGED
      }
    }

    InternalRow(newStart, end - trimSize, UTF8String.fromString(allAlleles(0)), ArrayData.toArrayData(allAlleles.tail.map(UTF8String.fromString(_))), UTF8String.fromString(flag))

  }

  private val PAD_WINDOW_SIZE = 100

  val FLAG_ERROR = "Error"
  val FLAG_CHANGED = "Changed"
  val FLAG_UNCHANGED = "Unchanged"

  val normalizedStartField = StructField("normalizedStart", LongType)
  val normalizedEndField = StructField("normalizedEnd", LongType)
  val normalizedRefAlleleField = StructField("normalizedReferenceAllele", StringType)
  val normalizedAlternateAllelesField = StructField("normalizedAlternateAlleles", ArrayType(StringType))
  val normalizationFlagField = StructField("normalizationFlag", StringType)

  val normalizationSchema = StructType(
    Seq(
      normalizedStartField,
      normalizedEndField,
      normalizedRefAlleleField,
      normalizedAlternateAllelesField,
      normalizationFlagField
    )
  )
}
