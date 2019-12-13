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

import com.google.common.annotations.VisibleForTesting
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext._
import htsjdk.variant.vcf.VCFHeader
import io.projectglow.common.GlowLogging
import io.projectglow.common.VariantSchemas._
import io.projectglow.vcf.{InternalRowToVariantContextConverter, VCFSchemaInferrer, VariantContextToInternalRowConverter}
import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLUtils}
import org.broadinstitute.hellbender.engine.{ReferenceContext, ReferenceDataSource}
import org.broadinstitute.hellbender.utils.SimpleInterval

import scala.collection.JavaConverters._
import scala.math.min

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
    val headerLineSet = VCFSchemaInferrer.headerLinesFromSchema(schema).toSet

    val dfAfterMaybeSplit = if (splitToBiallelic) {
      splitVariants(df)
    } else {
      df
    }

    // TODO: Implement normalization without using VariantContext
    val dfAfterMaybeNormalize = if (doNormalize) {
      val rddAfterNormalize = {
        // The following if-statement is temporary to make the normalization work.
        // After normalization is SQLified it will be dropped.
        if (splitToBiallelic) {
          dfAfterMaybeSplit.drop(infoFieldPrefix + oldMultiallelicFieldName)
        } else {
          dfAfterMaybeSplit
        }
      }.queryExecution.toRdd.mapPartitions { it =>
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

      SQLUtils.internalCreateDataFrame(df.sparkSession, rddAfterNormalize, schema, false)

    } else {
      dfAfterMaybeSplit
    }

    dfAfterMaybeNormalize

  }

  /**
   * Generates a new DataFrame by splitting the variants in the input DataFrame similar to what vt decompose does
   *
   * @param variantDf
   * @return dataframe of split variants
   */
  @VisibleForTesting
  private[normalizevariants] def splitVariants(variantDf: DataFrame): DataFrame = {

    if (variantDf.schema.fieldNames.contains("attributes")) {
      // TODO: Unflattened INFO field splitting
      logger.warn(
        "The variant splitting transformer does not split unflattened INFO fields within the attributes column.")
    }

    // Update splitFromMultiAllelic column, add INFO_OLD_MULTIALLELIC column (see vt decompose)
    // and posexplode alternateAlleles column
    val dfAfterAltAlleleSplit = variantDf
      .withColumn(
        splitFromMultiAllelicField.name,
        when(size(col(alternateAllelesField.name)) > 1, lit(true)).otherwise(lit(false))
      )
      .withColumn(
        infoFieldPrefix + oldMultiallelicFieldName,
        when(
          col(splitFromMultiAllelicField.name),
          concat(
            col(contigNameField.name),
            lit(":"),
            expr(s"${startField.name} + 1"),
            lit(":"),
            concat_ws("/", col(s"${refAlleleField.name}"), col(s"${alternateAllelesField.name}"))
          )
        ).otherwise(lit(null))
      )
      .select(
        col("*"),
        posexplode(col(alternateAllelesField.name))
          .as(Array(splitAlleleIdxFieldName, splitAllelesFieldName))
      )

    // Split INFO fields
    val dfAfterInfoSplit = splitInfoFields(dfAfterAltAlleleSplit)

    // register the udf that genotypes splitter uses
    variantDf
      .sqlContext
      .udf
      .register(
        "likelihoodSplitUdf",
        (numAlleles: Int, ploidy: Int, alleleIdx: Int) =>
          refAltColexOrderIdxArray(numAlleles, ploidy, alleleIdx)
      )

    // split genotypes fields, update alternateAlleles field, and drop the columns resulting from posexplode
    splitGenotypeFields(dfAfterInfoSplit)
      .withColumn(alternateAllelesField.name, array(col(splitAllelesFieldName)))
      .drop(splitAlleleIdxFieldName, splitAllelesFieldName)

  }

  /**
   * Generates a new DataFrame by splitting the info fields, based on splitAlleleIdx
   * field generated by posexplode
   * @param variantDf
   * @return dataframe with split info fields
   */
  @VisibleForTesting
  private[normalizevariants] def splitInfoFields(variantDf: DataFrame): DataFrame = {
    variantDf
      .schema
      .filter(field =>
        field.name.startsWith(infoFieldPrefix) && field.dataType.isInstanceOf[ArrayType])
      .foldLeft(
        variantDf
      )(
        (df, field) =>
          df.withColumn(
            field.name,
            when(
              col(splitFromMultiAllelicField.name) &&
              size(col(field.name)) === size(col(alternateAllelesField.name)),
              array(expr(s"${field.name}[$splitAlleleIdxFieldName]"))
            ).otherwise(col(field.name))
          )
      )
  }

  /**
   * Generates a new DataFrame by splitting the genotypes subfields, based on splitAlleleIdx
   * field generated by posexplode
   * @param variantDf
   * @return dataframe with split genotype subfields
   */
  @VisibleForTesting
  private[normalizevariants] def splitGenotypeFields(variantDf: DataFrame): DataFrame = {

    // get genotypes schema
    val genotypesFieldDataType = variantDf
      .schema
      .fields
      .find(_.name == genotypesFieldName)
      .get
      .dataType

    if (!genotypesFieldDataType.isInstanceOf[ArrayType]) {
      throw new ClassCastException("Genotypes field is not of ArrayType.")
    }

    val genotypesFieldElementType = genotypesFieldDataType.asInstanceOf[ArrayType].elementType

    if (!genotypesFieldElementType.isInstanceOf[StructType]) {
      throw new ClassCastException("Genotypes field element is not of StructType.")
    }

    val gSchema = genotypesFieldElementType.asInstanceOf[StructType]

    // pull out genotypes subfields as new columns
    val withExtractedFields = gSchema
      .fields
      .foldLeft(variantDf)((df, field) =>
        df.withColumn(field.name, expr(s"transform(${genotypesFieldName}, g -> g.${field.name})")))
      .drop(genotypesFieldName)

    // update pulled-out genotypes columns, zip them back together as the new genotypes column,
    // and drop the pulled-out columns
    gSchema
      .fields
      .foldLeft(withExtractedFields)(
        (df, field) =>
          field match {
            case f
                if structFieldsEqualExceptNullability(genotypeLikelihoodsField, f) |
                structFieldsEqualExceptNullability(phredLikelihoodsField, f) |
                structFieldsEqualExceptNullability(posteriorProbabilitiesField, f) =>
              // update genotypes subfields that have colex order using the udf
              df.withColumn(
                f.name,
                when(
                  col(splitFromMultiAllelicField.name),
                  expr( // TODO: Hard code the result of udf for numAlleles = 2, 3, 4 and ploidy = 2
                    s"""transform(${f.name}, c ->
                       | filter(
                       | transform(
                       | c, (x, idx) -> if (array_contains(
                       | likelihoodSplitUdf(size(${alternateAllelesField.name}) + 1,
                       | size(${callsField.name}[0]), $splitAlleleIdxFieldName + 1), idx), x, null)),
                       | x -> !isnull(x)))""".stripMargin)
                ).otherwise(col(f.name))
              )

            case f if structFieldsEqualExceptNullability(callsField, f) =>
              // update GT calls subfield
              df.withColumn(
                f.name,
                when(
                  col(splitFromMultiAllelicField.name),
                  expr(
                    s"transform(${f.name}, " +
                    s"c -> transform(c, x -> if(x == 0, x, if(x == $splitAlleleIdxFieldName + 1, 1, -1))))"
                  )
                ).otherwise(col(f.name))
              )

            case f if f.dataType.isInstanceOf[ArrayType] =>
              // update any ArrayType field with number of elements equal to number of alt alleles
              df.withColumn(
                f.name,
                when(
                  col(splitFromMultiAllelicField.name),
                  expr(
                    s"transform(${f.name}, c -> if(size(c) == size(${alternateAllelesField.name}) + 1," +
                    s" array(c[0], c[$splitAlleleIdxFieldName + 1]), null))"
                  )
                ).otherwise(col(f.name))
              )

            case _ => df
          }
      )
      .withColumn(genotypesFieldName, arrays_zip(gSchema.fieldNames.map(col(_)): _*))
      .drop(gSchema.fieldNames: _*)
  }

  /**
   * Given the total number of (ref and alt) alleles (numAlleles), ploidy, and the index an alt allele of interest
   * (altAlleleIdx), generates an array of indices of genotypes that only include the ref allele and/or that alt allele
   * of interest in the colex ordering of all possible genotypes. The function is general and correctly calculates
   * the index array for any given set of values for its arguments.
   *
   * Example:
   * Assume numAlleles = 3 (say A,B,C), ploidy = 2, and altAlleleIdx = 2 (i.e., C)
   * Therefore, colex ordering of all possible genotypes is: AA, AB, BB, AC, BC, CC
   * and for example refAltColexOrderIdxArray(3, 2, 2) = Array(0, 3, 5)
   *
   * @param numAlleles   : total number of alleles (ref and alt)
   * @param ploidy       : ploidy
   * @param altAlleleIdx : index of alt allele of interest
   * @return array of indices of genotypes that only include the ref allele and alt allele
   *         of interest in the colex ordering of all possible genotypes.
   */
  @VisibleForTesting
  private[normalizevariants] def refAltColexOrderIdxArray(
      numAlleles: Int,
      ploidy: Int,
      altAlleleIdx: Int): Array[Int] = {

    if (ploidy < 1) {
      throw new IllegalArgumentException("Ploidy must be at least 1.")
    }
    if (numAlleles < 2) {
      throw new IllegalArgumentException(
        "Number of alleles must be at least 2 (one REF and at least one ALT).")
    }
    if (altAlleleIdx > numAlleles - 1 || altAlleleIdx < 1) {
      throw new IllegalArgumentException(
        "Alternate allele index must be at least 1 and at most one less than number of alleles.")
    }

    if (ploidy == 1) {
      Array(0, altAlleleIdx)
    } else {

      Array(0) ++ refAltColexOrderIdxArray(altAlleleIdx + 1, ploidy - 1, altAlleleIdx)
        .map(e =>
          e +
          nChooseR(altAlleleIdx + ploidy - 1, ploidy) // The index at which allele altAlleleIdx appears for the first time in the colex order.
        )
    }
  }

  /** calculates n choose r
   *
   * @param n: total number of objects
   * @param r: number of selected objects
   * @return n choose r
   */
  @VisibleForTesting
  private[normalizevariants] def nChooseR(n: Int, r: Int): Int = {
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

  @VisibleForTesting
  private[normalizevariants] val splitAlleleIdxFieldName = "splitAlleleIdx"

  @VisibleForTesting
  private[normalizevariants] val splitAllelesFieldName = "splitAlleles"

  private val oldMultiallelicFieldName = "OLD_MULTIALLELIC"

}
