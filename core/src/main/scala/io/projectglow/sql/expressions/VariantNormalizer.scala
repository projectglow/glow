package io.projectglow.sql.expressions

import java.nio.file.Paths

import com.google.common.annotations.VisibleForTesting
import htsjdk.samtools.reference.IndexedFastaSequenceFile
import htsjdk.variant.variantcontext._
import io.projectglow.common.GlowLogging
import io.projectglow.common.VariantSchemas.alternateAllelesField
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, when}
import org.broadinstitute.hellbender.engine.{ReferenceContext, ReferenceDataSource}
import org.broadinstitute.hellbender.utils.SimpleInterval

import scala.math.min

object VariantNormalizer extends GlowLogging {

  /**
   * Normalizes the input DataFrame of variants and outputs them as a Dataframe
   *
   * @param df                   : Input dataframe of variants
   * @param refGenomePathString  : Path to the underlying reference genome of the variants
   * @return normalized DataFrame
   */
  def normalize(df: DataFrame, refGenomePathString: Option[String]): DataFrame = {

    if (refGenomePathString.isEmpty) {
      throw new IllegalArgumentException("Reference genome path not provided!")
    }


    /*
    if (!new File(refGenomePathString.get).exists()) {
      throw new IllegalArgumentException("The reference file was not found!")
    }

    val schema = df.schema
    val headerLineSet = VCFSchemaInferrer.headerLinesFromSchema(schema).toSet
    val validationStringency = ValidationStringency.valueOf("SILENT")

    val splitFromMultiallelicColumnIdx =
      df.schema.fieldNames.indexOf(splitFromMultiAllelicField.name)

    // TODO: Implement normalization without using VariantContext
    val rddAfterNormalize = df.queryExecution.toRdd.mapPartitions { it =>
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

      val refGenomeDataSource = Option(ReferenceDataSource.of(Paths.get(refGenomePathString.get)))

      it.map { row =>
        val isFromSplit = row.getBoolean(splitFromMultiallelicColumnIdx)
        internalRowToVariantContextConverter.convert(row) match {

          case Some(vc) =>
            variantContextToInternalRowConverter
              .convertRow(VariantNormalizer.normalizeVC(vc, refGenomeDataSource.get), isFromSplit)
          case None => row

        }
      }
    }
    SQLUtils.internalCreateDataFrame(df.sparkSession, rddAfterNormalize, schema, false)
  */

    normalizeVariants(df)
  }

  def normalizeVariants(variantDf: DataFrame): DataFrame = {

    // Update splitFromMultiAllelic column, add INFO_OLD_MULTIALLELIC column (see vt decompose)
    // and posexplode alternateAlleles column
    val dfAfterNormalize = variantDf
      .withColumn(alternateAllelesField.name,
        when(
          !concat_ws("", col(alternateAllelesField.name)).rlike(".*[<|>|*].*"),
          concat_ws("", col(alternateAllelesField.name)) // s"normalizeVariant(${alternateAllelesField.name}")
        )
      )
    dfAfterNormalize
  }









  /**
   * normalizes a single VariantContext by checking some conditions and then calling realignAlleles
   *
   * @param vc
   * @param refGenomeDataSource
   * @return normalized VariantContext
   */
   def normalizeVariant(
                                      start: Int,
                                      end: Int,
                                      refAllele: String,
                                      altAlleles: Array[String],
                                      refGenomeIndexedFasta: IndexedFastaSequenceFile): AlleleBlock = {

    val newAllele
    if (refAllele.length == 1 && altAlleles.forall(a => a.length == 1)) {
      altAlleles
    } else if (altAlleles.length == 0) {
      // if only one allele and longer than one base, trim to the
      // first base
      newRefAllel = refAllele(0)
    }

    /*
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
    */

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
