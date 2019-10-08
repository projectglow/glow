package org.projectglow.core.transformers.normalizevariants

import java.io.File
import java.nio.file.Paths

import scala.collection.JavaConverters._
import scala.math.min

import com.google.common.annotations.VisibleForTesting
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext._
import htsjdk.variant.vcf.VCFHeader
import org.apache.spark.sql.{DataFrame, SQLUtils}
import org.broadinstitute.hellbender.engine.{ReferenceContext, ReferenceDataSource}
import org.broadinstitute.hellbender.tools.walkers.genotyper.GenotypeAssignmentMethod
import org.broadinstitute.hellbender.utils.SimpleInterval
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils

import org.projectglow.core.common.HLSLogging
import org.projectglow.core.vcf.{InternalRowToVariantContextConverter, VCFFileWriter, VariantContextToInternalRowConverter}

private[projectglow] object VariantNormalizer extends HLSLogging {

  /**
   * Normalizes the input DataFrame of variants and outputs them as a Dataframe; Optionally
   * splits the multi-allelic variants to bi-allelics before normalization
   *
   * @param df                   : Input dataframe of variants
   * @param refGenomePathString  : Path to the underlying reference genome of the variants
   * @param validationStringency : ValidationStrigency as defined in htsjdk.samtools
   * @param doNormalize          : Whether to do normalization or not
   * @param splitToBiallelic     : Whether to split multiallelics or not
   * @return Split and/or normalized dataframe
   */
  def normalize(
      df: DataFrame,
      refGenomePathString: Option[String],
      validationStringency: ValidationStringency,
      doNormalize: Boolean,
      splitToBiallelic: Boolean): DataFrame = {

    if (doNormalize) {
      if (refGenomePathString.isEmpty) {
        throw new IllegalArgumentException("Reference genome path not provided!")
      }
      if (!new File(refGenomePathString.get).exists()) {
        throw new IllegalArgumentException("The reference file was not found!")
      }
    }

    val schema = df.schema

    val headerLineSet =
      VCFFileWriter
        .parseHeaderLinesAndSamples(
          Map("vcfHeader" -> "infer"),
          None,
          schema,
          df.sparkSession.sparkContext.hadoopConfiguration
        )
        ._1

    // flatmap InternalRows to (optionally split) normalized rows; coverts each InternalRow to a
    // VariantContext, performs splitting and normalization on the VariantContext, converts it
    // back to InternalRow(s)

    val mapped = df.queryExecution.toRdd.mapPartitions { it =>
      val vcfHeader = new VCFHeader(headerLineSet.asJava)

      val variantContextToInternalRowConverter =
        new VariantContextToInternalRowConverter(
          vcfHeader,
          schema,
          validationStringency
        )

      val internalRowToVariantContextConverter =
        new InternalRowToVariantContextConverter(
          schema,
          headerLineSet,
          validationStringency
        )

      internalRowToVariantContextConverter.validate()

      val refGenomeDataSource = if (doNormalize) {
        Option(ReferenceDataSource.of(Paths.get(refGenomePathString.get)))
      } else {
        None
      }

      it.flatMap { row =>
        internalRowToVariantContextConverter.convert(row) match {

          case Some(vc) =>
            val vcStreamAfterMaybeSplit: Stream[VariantContext] =
              // Strangely, Array, List, Seq, or Iterator do not work correctly!
              // Line-by-line debugging shows that the issue is after the splitting.
              // Assume a multi-allelic vc is split into two bi-allelic vcs. If the resulting two
              // vcs are stored in any collection other than Stream, then the map function that
              // applies the variantContextToInternalRowConverter.convertRow on them puts two
              // copies of the InternalRow generated from the first vc in the result instead of one
              // copy of each!
              if (splitToBiallelic) {
                splitVC(vc)
              } else {
                Stream(vc)
              }

            val isFromSplit = vcStreamAfterMaybeSplit.length > 1

            val vcStreamAfterMaybeNormalize = if (doNormalize) {
              vcStreamAfterMaybeSplit.map(vc =>
                VariantNormalizer.normalizeVC(vc, refGenomeDataSource.get))
            } else {
              vcStreamAfterMaybeSplit
            }

            vcStreamAfterMaybeNormalize.map(
              variantContextToInternalRowConverter.convertRow(_, isFromSplit)
            )

          case None => Stream(row)

        }
      }
    }

    SQLUtils.internalCreateDataFrame(df.sparkSession, mapped, schema, false)

  }

  /**
   * Splits a vc to bi-alleleic using gatk splitting function
   *
   * @param vc
   * @return a Stream of split vc's
   */
  private def splitVC(vc: VariantContext): Stream[VariantContext] = {
    GATKVariantContextUtils
      .splitVariantContextToBiallelics(
        vc,
        false,
        GenotypeAssignmentMethod.BEST_MATCH_TO_ORIGINAL,
        false
      )
      .asScala
      .toStream
  }

  /**
   * Encapsulates all alleles, start, and end of a variant to used by the VC normalizer
   *
   * @param alleles
   * @param start
   * @param end
   */
  @VisibleForTesting
  private[normalizevariants] case class AlleleBlock(alleles: Seq[Allele], start: Int, end: Int)

  /**
   * normalizes a single VariantContext by checking some conditions and then calling realignAlleles
   *
   * @param vc
   * @param refGenomePathString
   * @return: normalized VariantContext
   */
  private def normalizeVC(
      vc: VariantContext,
      refGenomeDataSource: ReferenceDataSource): VariantContext = {

    if (vc.getNAlleles < 1) {
      // if no alleles, throw exception
      logger.info("Cannot compute right-trim size for an empty allele list...")
      throw new IllegalArgumentException
    } else if (vc.isSNP) {
      // if a SNP, do nothing
      vc
    } else if (vc.getNAlleles == 1) {
      // if only one allele and longer than one base, trim to the
      // first base
      val ref = vc.getReference
      if (ref.length > 1) {
        val newBase = ref.getBases()(0)
        val trimmedAllele = Allele.create(newBase, ref.isReference)
        new VariantContextBuilder(vc)
          .start(vc.getStart)
          .stop(vc.getStart) // end is equal to start.
          .alleles(Seq(trimmedAllele).asJava)
          .make
      } else {
        vc
      }
    } else {
      val alleles = vc.getAlleles.asScala
      if (alleles.exists(_.isSymbolic)) {
        // if any of the alleles is symbolic, do nothing
        vc
      } else {
        // Create ReferenceDataSource of the reference genome and the AlleleBlock and pass
        // to realignAlleles

        updateVCWithNewAlleles(
          vc,
          realignAlleles(
            AlleleBlock(alleles, vc.getStart, vc.getEnd),
            refGenomeDataSource,
            vc.getContig
          )
        )

      }
    }
  }

  /**
   * Updates the alleles and genotypes in a VC with new alleles
   *
   * @param originalVC
   * @param newAlleleBlock
   * @return updated VariantContext
   */
  private def updateVCWithNewAlleles(
      originalVC: VariantContext,
      newAlleleBlock: AlleleBlock): VariantContext = {

    val originalAlleles = originalVC.getAlleles.asScala
    val newAlleles = newAlleleBlock.alleles

    var alleleMap = Map[Allele, Allele]()

    for (i <- 0 to originalVC.getNAlleles - 1) {
      alleleMap += originalAlleles(i) -> newAlleles(i)
    }

    val originalGenotypes = originalVC.getGenotypes.asScala
    val updatedGenotypes = GenotypesContext.create(originalGenotypes.size)
    for (genotype <- originalGenotypes) {
      val updatedGenotypeAlleles =
        genotype.getAlleles.asScala.map(a => alleleMap.getOrElse(a, a)).asJava
      updatedGenotypes.add(new GenotypeBuilder(genotype).alleles(updatedGenotypeAlleles).make)
    }

    new VariantContextBuilder(originalVC)
      .start(newAlleleBlock.start)
      .stop(newAlleleBlock.end)
      .alleles(newAlleles.asJava)
      .genotypes(updatedGenotypes)
      .make
  }

  /**
   * Contains the main normalization logic. Normalizes an AlleleBlock by left aligning and
   * trimming its alleles and adjusting its new start and end.
   *
   * The algorithm has a logic similar to bcftools:
   *
   * It starts from the rightmost base of all alleles and scans one base at a time incrementing
   * trimSize and nTrimmedBasesBeforeNextPadding as long as the bases of all alleles at that
   * position are the same. If the beginning of any of the alleles is reached, all alleles are
   * padded on the left by PAD_WINDOW_SIZE bases by reading from the reference genome amd
   * nTrimmedBaseBeforeNextPadding is reset. The process continues until a position is reached
   * where all alleles do not have the same base or the beginning of the contig is reached. Next
   * trimming from left starts and all bases common among all alleles from left are trimmed.
   * Start and end of the AllleleBlock are adjusted accordingly during the process.
   *
   * @param unalignedAlleleBlock
   * @param refGenomeDataSource
   * @param contig : contig of the AlleleBlock
   * @return normalized AlleleBlock
   */
  @VisibleForTesting
  private[normalizevariants] def realignAlleles(
      unalignedAlleleBlock: AlleleBlock,
      refGenomeDataSource: ReferenceDataSource,
      contig: String): AlleleBlock = {

    // Trim from right
    var trimSize = 0 // stores total trimSize from right
    var nTrimmedBasesBeforeNextPadding = 0 // stores number of bases trimmed from right before
    // next padding
    var newStart = unalignedAlleleBlock.start
    var alleles = unalignedAlleleBlock.alleles
    var firstAlleleBaseFromRight = alleles(0).getBases()(
      alleles(0).length
      - nTrimmedBasesBeforeNextPadding - 1
    )

    while (alleles.forall(
        a =>
          a.getBases()(a.length() - nTrimmedBasesBeforeNextPadding - 1) ==
          firstAlleleBaseFromRight
      )) {
      // Last base in all alleles are the same

      var padSeq = Array[Byte]()
      var nPadBases = 0

      if (alleles
          .map(_.length)
          .min == nTrimmedBasesBeforeNextPadding + 1) {
        // if
        // beginning of any allele is reached, trim from right what
        // needs to be trimmed so far, and pad to the left
        if (newStart > 1) {
          nPadBases = min(PAD_WINDOW_SIZE, newStart - 1)

          val refGenomeContext = new ReferenceContext(
            refGenomeDataSource,
            new SimpleInterval(contig, newStart - 1, newStart - 1)
          )

          refGenomeContext.setWindow(nPadBases - 1, 0)

          padSeq ++= refGenomeContext.getBases()

        } else {
          nTrimmedBasesBeforeNextPadding -= 1
        }

        alleles = alleles.map { a =>
          Allele.create(
            padSeq ++ a
              .getBaseString()
              .dropRight(nTrimmedBasesBeforeNextPadding + 1)
              .getBytes(),
            a.isReference
          )
        }

        trimSize += nTrimmedBasesBeforeNextPadding + 1

        newStart -= nPadBases

        nTrimmedBasesBeforeNextPadding = 0

      } else {

        nTrimmedBasesBeforeNextPadding += 1

      }

      firstAlleleBaseFromRight = alleles(0).getBases()(
        alleles(0).length
        - nTrimmedBasesBeforeNextPadding - 1
      )
    }

    // trim from left
    var nLeftTrimBases = 0
    var firstAlleleBaseFromLeft = alleles(0).getBases()(nLeftTrimBases)
    val minAlleleLength = alleles.map(_.length).min

    while (nLeftTrimBases < minAlleleLength - nTrimmedBasesBeforeNextPadding - 1
      && alleles.forall(_.getBases()(nLeftTrimBases) == firstAlleleBaseFromLeft)) {

      nLeftTrimBases += 1

      firstAlleleBaseFromLeft = alleles(0).getBases()(nLeftTrimBases)
    }

    alleles = alleles.map { a =>
      Allele.create(
        a.getBaseString()
          .drop(nLeftTrimBases)
          .dropRight(nTrimmedBasesBeforeNextPadding)
          .getBytes(),
        a.isReference
      )

    }

    trimSize += nTrimmedBasesBeforeNextPadding

    AlleleBlock(
      alleles,
      newStart + nLeftTrimBases,
      unalignedAlleleBlock.end - trimSize
    )

  }

  private val PAD_WINDOW_SIZE = 100

}
