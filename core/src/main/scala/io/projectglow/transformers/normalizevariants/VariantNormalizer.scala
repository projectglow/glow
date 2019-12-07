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

import java.io.File
import java.nio.file.Paths

import scala.collection.JavaConverters._
import scala.math.min
import com.google.common.annotations.VisibleForTesting
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext._
import htsjdk.variant.vcf.VCFHeader
import org.apache.spark.sql.{DataFrame, SQLUtils}
import org.apache.spark.sql.functions._
import org.broadinstitute.hellbender.engine.{ReferenceContext, ReferenceDataSource}
import org.broadinstitute.hellbender.utils.SimpleInterval
import io.projectglow.common.GlowLogging
import io.projectglow.common.VariantSchemas._
import io.projectglow.vcf.{InternalRowToVariantContextConverter, VCFFileWriter, VariantContextToInternalRowConverter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.catalyst.InternalRow

private[projectglow] object VariantNormalizer extends GlowLogging {

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

    val dfAfterMaybeSplit = if (splitToBiallelic) {
      splitVariants(df)
    } else {
      df
    }

    // TODO: Implement normalization without using VariantContext
    val rddAfterMaybeNormalize = if (doNormalize) {
      dfAfterMaybeSplit.queryExecution.toRdd.mapPartitions { it =>
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
          val isFromSplit = false
          internalRowToVariantContextConverter.convert(row) match {

            case Some(vc) =>
              variantContextToInternalRowConverter
                .convertRow(VariantNormalizer.normalizeVC(vc, refGenomeDataSource.get), isFromSplit)
            case None => row
          }
        }
      }
    } else {
      dfAfterMaybeSplit.queryExecution.toRdd
    }

    SQLUtils.internalCreateDataFrame(df.sparkSession, rddAfterMaybeNormalize, schema, false)
  }

  /**
   * Generates a new DataFrame by splitting the variants in the input DataFrame
   *
   * @param inputDf
   * @return a Stream of split vc's
   */
  def splitVariants(inputDf: DataFrame): DataFrame = {

    val variantDf = if (inputDf.schema.fieldNames.contains("attributes")) {
      // TODO: What to do when INFO fields are not flattened?
      inputDf // WIP
    } else {
      inputDf
    }

    // Add/replace splitFromMultiAllelic column and explode alternateAlleles column
    val dfAfterAltAlleleSplit = variantDf
      .withColumn(
        splitFromMultiAllelicField.name,
        when(size(col(alternateAllelesField.name)) > 1, true).otherwise(false)
      )
      .select(
        col("*"),
        posexplode(col(alternateAllelesField.name)).as(Array("splitAlleleIdx", "splitAlleles"))
      )

    /*
    // Seq of functions to be applied on ArrayTyped INFO fields to split them if their size is equal to number of alternate alleles
    val infoFieldsSplittingFunctions: Seq[DataFrame => DataFrame] = variantDf
      .schema
      .filter(field =>
        field.name.startsWith(infoFieldPrefix) && field.dataType.isInstanceOf[ArrayType])
      .map(field =>
        (f: DataFrame) => {
          f.withColumn(
            field.name,
            when(
              size(col(field.name)) === size(col(alternateAllelesField.name)),
              array(expr(s"${field.name}[splitAlleleIdx]"))).otherwise(col(field.name)))
        })

    // val genotypesFieldFunctions = Seq[DataFrame => DataFrame] = variantDf.

    // Update INFO fields by applying splitting functions.
    var dfAfterInfoSplit = infoFieldsSplittingFunctions
      .foldLeft(
        dfAfterAltAlleleSplit
      )((df, fn) => fn(df))
      .withColumn(alternateAllelesField.name, array(col("splitAlleles"))) // replace alternateAlleles with splitAlleles
      .drop("splitAlleleIdx", "splitAlleles") // drop helper columns
     */

    val dfAfterInfoSplit = variantDf
      .schema
      .filter(field =>
        field.name.startsWith(infoFieldPrefix) && field.dataType.isInstanceOf[ArrayType])
      .foldLeft(
        dfAfterAltAlleleSplit
      )(
        (df, field) =>
          df.withColumn(
            field.name,
            when(
              size(col(field.name)) === size(col(alternateAllelesField.name)),
              array(expr(s"${field.name}[splitAlleleIdx]"))).otherwise(col(field.name))
          )
      )

    // TODO: Update genotypes
    val gSchema = variantDf
      .schema
      .fields
      .find(_.name == "genotypes")
      .get
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]

    // pull out gt field
    val withExtractedFields = gSchema
      .fields
      .foldLeft(dfAfterInfoSplit)((df, field) =>
        df.withColumn(field.name, expr(s"transform(genotypes, g -> g.${field.name})")))
      // .withColumn(sampleIdField.name, expr(s"transform(genotypes, g -> g.${sampleIdField.name})"))
      // .withColumn(phasedField.name, expr(s"transform(genotypes, g -> g.${phasedField.name})"))
      // .withColumn(callsField.name, expr(s"transform(genotypes, g -> g.${callsField.name})"))
      .drop("genotypes")

    val dfAfterGenotypesSplit = gSchema
      .fields
      .foldLeft(withExtractedFields)(
        (df, field) =>
          field match {
            case f
                if structFieldsEqualExceptNullability(genotypeLikelihoodsField, f) |
                structFieldsEqualExceptNullability(phredLikelihoodsField, f) |
                structFieldsEqualExceptNullability(posteriorProbabilitiesField, f) =>
              df
            case f if structFieldsEqualExceptNullability(callsField, f) =>
              df.withColumn(
                f.name,
                expr(
                  s"transform(${f.name}, c -> transform(c, x -> if(x == 0, x, if(x == splitAlleleIdx + 1, 1, -1))))"))
            case f if f.dataType.isInstanceOf[ArrayType] =>
              df.withColumn(
                f.name,
                expr(
                  s"transform(${f.name}, c -> if(size(c) == size(${alternateAllelesField.name}) + 1, array(c[0], c[splitAlleleIdx + 1]), null))"))
            case _ => df
          }
      ) // .withColumn(alternateAllelesField.name, array(col("splitAlleles"))) // replace alternateAlleles with splitAlleles
    //     .drop("splitAlleleIdx", "splitAlleles") // drop helper columns

    // .withColumn("genotypes", expr(s"arrays_zip(${gSchema.fieldNames.mkString(",")})"))// .withColumn("calls", expr("transform(calls, c -> transform(c, x -> x + 1))"))
    // .drop(gSchema.fieldNames: _*)

    /*

    for (i <- 0 to dfAfterInfoSplit
        .limit(1)
        .select(size(col("genotypes")))
        .collect()(0)
        .getInt(0)) {
      // dfAfterInfoSplit = dfAfterInfoSplit.withColumn(s"genotypes_${i}", expr(s"(genotypes[$i])"))
      dfAfterInfoSplit = dfAfterInfoSplit.select(col("*"), expr(s"expand_struct(genotypes[$i])"))
    }
     */

    dfAfterGenotypesSplit
  }

  @VisibleForTesting
  private[normalizevariants] def genotypeLikelihoodsSplitIdxArray(
      numAlleles: Int,
      ploidy: Int,
      alleleIdx: Int): Array[Int] = {

    if (ploidy == 1) {
      Array(alleleIdx)
    } else {
      val firstAppIdxArray = alleleFirstAppearanceIdxArray(numAlleles, ploidy)
      var idxArray = (firstAppIdxArray(alleleIdx) to firstAppIdxArray(alleleIdx + 1) - 1).toArray
      var tempNumAllele = alleleIdx + 2

      while (tempNumAllele <= numAlleles) {
        idxArray ++= genotypeLikelihoodsSplitIdxArray(tempNumAllele, ploidy - 1, alleleIdx).map(e =>
          e + firstAppIdxArray(tempNumAllele - 1))
        tempNumAllele += 1
      }
      idxArray
    }
  }

  @VisibleForTesting
  private[normalizevariants] def alleleFirstAppearanceIdxArray(
      numAlleles: Int,
      ploidy: Int): Array[Int] = {
    0 +: (1 to numAlleles).toArray.map(i => choose(i + ploidy - 1, ploidy))
  }

  @VisibleForTesting
  private[normalizevariants] def choose(n: Int, r: Int): Int = {
    if (r > n) {
      0
    } else if (r == n) {
      1
    } else if (r == 0) {
      1
    } else {

      val sr = if (r > (n >> 1)) {
        n - r
      } else {
        r
      }

      var num = n
      var denum = 1
      var i = 1

      while (i < sr) {
        num *= n - i
        denum *= i + 1
        i += 1
      }
      num / denum
    }
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
   * @param refGenomeDataSource
   * @return normalized VariantContext
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
