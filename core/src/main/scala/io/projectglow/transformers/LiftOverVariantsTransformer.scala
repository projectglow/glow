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

package io.projectglow.transformers

import java.io.File
import java.util.{ArrayList => JArrayList, Collections, HashMap => JHashMap, List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable

import htsjdk.samtools.ValidationStringency
import htsjdk.samtools.liftover.LiftOver
import htsjdk.samtools.reference.{ReferenceSequence, ReferenceSequenceFileFactory}
import htsjdk.samtools.util.Interval
import htsjdk.variant.variantcontext.{Allele, Genotype, GenotypeBuilder, GenotypesContext, VariantContext, VariantContextBuilder}
import htsjdk.variant.vcf._
import org.apache.commons.lang.ArrayUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Literal, MutableProjection}
import org.apache.spark.sql.types.{ArrayType, BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLUtils}
import org.apache.spark.unsafe.types.UTF8String
import picard.util.LiftoverUtils
import picard.vcf.LiftoverVcf

import io.projectglow.DataFrameTransformer
import io.projectglow.common.{GenotypeFields, GlowLogging, VariantSchemas}
import io.projectglow.sql.expressions.LiftOverCoordinatesExpr
import io.projectglow.vcf.{InternalRowToVariantContextConverter, VCFSchemaInferrer, VariantContextToInternalRowConverter}

/**
 * Performs lift over from a variant on the reference sequence to a query sequence. Similar to
 * [[LiftOverCoordinatesExpr]], but with additional fields for allele liftover.
 *
 * The output schema consists of the input schema followed by the following fields:
 * - Flattened INFO columns containing extra information added during liftover (eg. swapped alleles)
 * - Status column
 *   - Success (Boolean)
 *   - Error message (if failed)
 */
class LiftOverVariantsTransformer extends DataFrameTransformer {

  import LiftOverVariantsTransformer._

  override def name: String = "lift_over_variants"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {
    val inputSchema = df.schema

    // Minimal fields required to construct a VariantContext
    SQLUtils.verifyHasFields(
      inputSchema,
      Seq(
        VariantSchemas.contigNameField,
        VariantSchemas.startField,
        VariantSchemas.endField,
        VariantSchemas.refAlleleField))
    val inputHeaderLines = VCFSchemaInferrer.headerLinesFromSchema(inputSchema).toSet
    val attributesToSwap = getAttributesToSwap(df.schema)
    val extendedAttributesToSwap = getExtendedAttributesToSwap(df.schema)
    val outputHeaderLines = inputHeaderLines ++ picardInfoHeaderLines
    val outputSchema = getOutputSchema(inputSchema)
    val liftOverStatusColIdx = outputSchema.length - 1

    val minMatchRatio = getMinMatchRatio(options)

    val internalRowRDD = df.queryExecution.toRdd.mapPartitions { it =>
      val internalRowToVcConverter = new InternalRowToVariantContextConverter(
        inputSchema,
        inputHeaderLines,
        ValidationStringency.LENIENT)
      val vcToInternalRowConverter = new VariantContextToInternalRowConverter(
        new VCFHeader(outputHeaderLines.asJava),
        outputSchema,
        ValidationStringency.LENIENT)
      val liftOver = getLiftOver(options)
      val refSeqMap = getRefSeqMap(options)

      // Project rows to the output schema
      val projection = makeMutableProjection(inputSchema)

      it.map { inputRow =>
        // Create a row with the output schema using the input row
        val inputRowInOutputSchema = projection(inputRow)

        val inputVcOpt = internalRowToVcConverter.convert(inputRow)
        val (baseRow, success, messageOpt) = if (inputVcOpt.isEmpty) {
          // If conversion fails, use the input row and set the added liftover INFO columns to null
          (inputRowInOutputSchema, false, Some(conversionErrorMessage))
        } else {
          liftVariantContext(
            inputVcOpt.get,
            liftOver,
            minMatchRatio,
            refSeqMap,
            attributesToSwap,
            extendedAttributesToSwap) match {
            case (Some(outputVc), None) =>
              // If liftover succeeds, convert the output row to the output schema using the input row as a prior
              val liftedRow =
                vcToInternalRowConverter.convertRow(outputVc, inputRowInOutputSchema, false)
              (liftedRow, true, None)
            case (None, Some(msg)) =>
              // If liftover fails, use the input row and set the added liftover INFO columns to null
              (inputRowInOutputSchema, false, Some(msg))
            case (_, _) =>
              throw new IllegalStateException(
                "Must have either lifted variant context or error message.")
          }
        }

        val liftoverStatus = InternalRow(success, messageOpt.map(UTF8String.fromString).orNull)
        baseRow.update(liftOverStatusColIdx, liftoverStatus)
        baseRow
      }
    }

    SQLUtils.internalCreateDataFrame(
      df.sparkSession,
      internalRowRDD,
      outputSchema,
      isStreaming = false)
  }
}

object LiftOverVariantsTransformer extends GlowLogging {
  val liftOverStatusColName = "liftOverStatus"
  val successColName = "success"
  val errorMessageColName = "errorMessage"
  val conversionErrorMessage = "Conversion failed"

  private val chainFileKey = "chainFile"
  private val minMatchRatioKey = "minMatchRatio"
  private val referenceFileKey = "referenceFile"

  private val statusStructField: StructField = StructField(
    liftOverStatusColName,
    new StructType(
      Array(
        StructField(successColName, BooleanType),
        StructField(errorMessageColName, StringType))))
  private val statusStructFieldLiteral = Literal(null, statusStructField.dataType)

  private val picardInfoHeaderLines: Seq[VCFInfoHeaderLine] = Seq(
    new VCFInfoHeaderLine(
      LiftoverUtils.SWAPPED_ALLELES,
      0,
      VCFHeaderLineType.Flag,
      "The REF and the ALT alleles have been swapped in liftover due to changes in the reference. " +
      "It is possible that not all INFO annotations reflect this swap, and in the genotypes, " +
      "only the GT, PL, and AD fields have been modified. You should check the TAGS_TO_REVERSE parameter that was used " +
      "during the LiftOver to be sure."),
    new VCFInfoHeaderLine(
      LiftoverUtils.REV_COMPED_ALLELES,
      0,
      VCFHeaderLineType.Flag,
      "The REF and the ALT alleles have been reverse complemented in liftover since the mapping from the " +
      "previous reference to the current one was on the negative strand.")
  )
  private val picardInfoStructFields: Seq[StructField] =
    picardInfoHeaderLines.map(VCFSchemaInferrer.getInfoFieldStruct)
  private val picardStructFieldLiterals = picardInfoStructFields.map { f =>
    Literal(null, f.dataType)
  }

  def getLiftOver(options: Map[String, String]): LiftOver = {
    val chainFile = options.getOrElse(
      chainFileKey,
      throw new IllegalArgumentException(
        "Must provide chain file from the reference sequence to the query sequence.")
    )
    new LiftOver(new File(chainFile))
  }

  def getMinMatchRatio(options: Map[String, String]): Double = {
    options.get(minMatchRatioKey).map(_.toDouble).getOrElse(0.95)
  }

  // Read a Fasta file once to create a map from contigName to the contig's reference bases
  def getRefSeqMap(options: Map[String, String]): Map[String, ReferenceSequence] = {
    val referenceFile = options.getOrElse(
      referenceFileKey,
      throw new IllegalArgumentException("Must provide reference file for the query sequence.")
    )
    val refSeqFile = ReferenceSequenceFileFactory.getReferenceSequenceFile(new File(referenceFile))

    val mmap = new mutable.HashMap[String, ReferenceSequence]
    var nextSeq = refSeqFile.nextSequence
    while (nextSeq != null) {
      mmap.put(nextSeq.getName, nextSeq)
      nextSeq = refSeqFile.nextSequence
    }
    mmap.toMap
  }

  // Tries to liftover a variant context.
  // - If this succeeds, returns (Some(VariantContext), None).
  // - If this fails, returns (None, Some("errorMessage")).
  def liftVariantContext(
      inputVc: VariantContext,
      liftOver: LiftOver,
      minMatchRatio: Double,
      refSeqMap: Map[String, ReferenceSequence],
      infoFieldsToSwap: Seq[String],
      formatFieldsToSwap: Seq[String]): (Option[VariantContext], Option[String]) = {

    val inputInterval = new Interval(
      inputVc.getContig,
      inputVc.getStart,
      inputVc.getEnd,
      false,
      inputVc.getContig + ":" + inputVc.getStart + "-" + inputVc.getEnd)
    val outputIntervalOpt = Option(liftOver.liftOver(inputInterval, minMatchRatio))

    if (outputIntervalOpt.isEmpty) {
      return (None, Some(LiftoverVcf.FILTER_NO_TARGET))
    }

    val outputInterval = outputIntervalOpt.get
    if (inputVc.getReference.length != outputInterval.length) {
      // Interval grew/shrank during liftover (due to straddling multiple intervals in the liftover chain)
      return (None, Some(LiftoverVcf.FILTER_INDEL_STRADDLES_TWO_INTERVALS))
    }

    val refSeqOpt = refSeqMap.get(outputInterval.getContig)
    if (refSeqOpt.isEmpty) {
      return (None, Some(LiftoverVcf.FILTER_NO_TARGET))
    }

    val refSeq = refSeqOpt.get
    val outputVcOpt = Option(
      LiftoverUtils.liftVariant(inputVc, outputInterval, refSeq, false, false))
    if (outputVcOpt.isEmpty) {
      return (None, Some(LiftoverVcf.FILTER_CANNOT_LIFTOVER_REV_COMP))
    }

    val outputVc = outputVcOpt.get
    val refStr = refSeq.getBaseString.substring(outputVc.getStart - 1, outputVc.getEnd)

    if (!refStr.equalsIgnoreCase(outputVc.getReference.getBaseString)) {
      if (outputVc.isBiallelic && outputVc.isSNP && refStr.equalsIgnoreCase(
          outputVc.getAlternateAllele(0).getBaseString)) {
        val swappedArrayFields =
          swapRefAlt(outputVc, infoFieldsToSwap, formatFieldsToSwap)
        return (Some(swappedArrayFields), None)
      }
      val attemptedLocus = s"${outputVc.getContig}:${outputVc.getStart}-${outputVc.getEnd}"
      return (
        None,
        Some(
          s"${LiftoverVcf.FILTER_MISMATCHING_REF_ALLELE}: ${LiftoverVcf.ATTEMPTED_LOCUS} $attemptedLocus"))
    }

    (Some(outputVc), None)
  }

  // Includes the same functionality as [[LiftoverUtils.swapRefAlt]]; also reverses array-based INFO/FORMAT fields
  def swapRefAlt(
      vc: VariantContext,
      infoFieldsToSwap: Seq[String],
      formatFieldsToSwap: Seq[String]): VariantContext = {
    val vcb = new VariantContextBuilder(vc)
    vcb.attribute(LiftoverUtils.SWAPPED_ALLELES, true)
    vcb.alleles(vc.getAlleles.get(1).getBaseString, vc.getAlleles.get(0).getBaseString)

    val alleleMap = new JHashMap[Allele, Allele]()

    // A mapping from the old allele to the new allele, to be used when fixing the genotypes
    alleleMap.put(vc.getAlleles.get(0), vcb.getAlleles.get(1))
    alleleMap.put(vc.getAlleles.get(1), vcb.getAlleles.get(0))

    // Drop INFO field MAX_AF
    LiftoverUtils.DEFAULT_TAGS_TO_DROP.asScala.foreach { k =>
      vcb.rmAttribute(k)
    }

    // Invert INFO field AF
    LiftoverUtils.DEFAULT_TAGS_TO_REVERSE.asScala.foreach { k =>
      if (vc.hasAttribute(k)) {
        vcb.attribute(k, vc.getAttributeAsDoubleList(k, 0).asScala.map(1 - _).toList.asJava)
      }
    }

    // Reverse other array-based INFO fields
    infoFieldsToSwap.foreach { k =>
      val arrList = vc.getAttributeAsList(k)
      Collections.reverse(arrList)
      vcb.attribute(k, arrList)
    }

    val swappedGenotypes = GenotypesContext.create(vc.getGenotypes().size())
    var i = 0
    while (i < vc.getGenotypes.size) {
      val genotype = vc.getGenotypes.get(i)

      val swappedAlleles = new JArrayList[Allele]()
      var j = 0
      while (j < genotype.getAlleles.size) {
        val allele = genotype.getAllele(j)
        if (allele.isNoCall) {
          swappedAlleles.add(allele)
        } else {
          swappedAlleles.add(alleleMap.get(allele))
        }
        j += 1
      }
      val gb = new GenotypeBuilder(genotype).alleles(swappedAlleles)

      // Special case: AD is not included in the extended attributes
      if (genotype.hasAD) {
        val ad = genotype.getAD
        ArrayUtils.reverse(ad)
        gb.AD(ad)
      }

      // Special case: PL is not included in the extended attributes
      if (genotype.hasPL) {
        val pl = genotype.getPL
        ArrayUtils.reverse(pl)
        gb.PL(pl)
      }

      // Reverse array-based FORMAT fields
      formatFieldsToSwap.foreach { k =>
        if (genotype.hasExtendedAttribute(k)) {
          val arrList = genotype.getExtendedAttribute(k).asInstanceOf[JList[AnyRef]]
          Collections.reverse(arrList)
          gb.attribute(k, arrList)
        }
      }
      swappedGenotypes.add(gb.make())
      i += 1
    }
    vcb.genotypes(swappedGenotypes).make
  }

  // The output schema consists of the input schema + INFO fields added by Picard + a liftOver status column
  def getOutputSchema(inputSchema: StructType): StructType = {
    StructType(inputSchema.fields ++ picardInfoStructFields :+ statusStructField)
  }

  def makeMutableProjection(inputSchema: StructType): MutableProjection = {
    val passThroughExpressions =
      inputSchema.zipWithIndex.map {
        case (f, ord) =>
          BoundReference(ordinal = ord, f.dataType, f.nullable)
      }

    GenerateMutableProjection.generate(
      passThroughExpressions ++ picardStructFieldLiterals :+ statusStructFieldLiteral)
  }

  def getArrayFields(fields: Seq[StructField]): Seq[String] = {
    fields.flatMap { f =>
      if (f.dataType.isInstanceOf[ArrayType]) {
        Some(f.name)
      } else {
        None
      }
    }
  }

  // Get array-based INFO fields
  def getAttributesToSwap(variantSchema: StructType): Seq[String] = {
    val infoFields = variantSchema.flatMap { f =>
      if (f.name.startsWith(VariantSchemas.infoFieldPrefix)) {
        val name = f.name.substring(VariantSchemas.infoFieldPrefix.length)
        if (LiftoverUtils
            .DEFAULT_TAGS_TO_DROP
            .contains(name) || LiftoverUtils.DEFAULT_TAGS_TO_REVERSE.contains(name)) {
          None
        } else {
          Some(f.copy(name = f.name.substring(VariantSchemas.infoFieldPrefix.length)))
        }
      } else {
        None
      }
    }
    getArrayFields(infoFields)
  }

  // Get array-based FORMAT fields
  def getExtendedAttributesToSwap(variantSchema: StructType): Seq[String] = {
    val genotypeSchemaOpt = InternalRowToVariantContextConverter.getGenotypeSchema(variantSchema)

    if (genotypeSchemaOpt.isEmpty) {
      return Seq.empty
    }

    val formatFields = genotypeSchemaOpt.get.flatMap { f =>
      val name = GenotypeFields.reverseAliases.getOrElse(f.name, f.name)
      // Special cases: FT, GT, GQ, DP, AD and PL are not included in the extended attributes
      if (Genotype.PRIMARY_KEYS.contains(name)) {
        None
      } else {
        Some(f.copy(name = name))
      }
    }

    getArrayFields(formatFields)
  }
}
