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

package io.projectglow.transformers.splitmultiallelics

import com.google.common.annotations.VisibleForTesting
import htsjdk.variant.vcf.VCFHeaderLineCount
import io.projectglow.common.GlowLogging
import io.projectglow.common.VariantSchemas._
import io.projectglow.vcf.{InternalRowToVariantContextConverter, VCFSchemaInferrer}
import org.apache.commons.math3.util.CombinatoricsUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType

private[projectglow] object VariantSplitter extends GlowLogging {

  /**
   * Generates a new DataFrame by splitting the variants in the input DataFrame similar to what "vt decompose -s" does
   * See https://genome.sph.umich.edu/wiki/Vt#Decompose for more info but note that the example shown there
   * does not exactly match the real behavior of vt decompose with the -s option.
   *
   * The summary of true behavior is as follows:
   *
   * Any given multiallelic row with n Alt alleles is split to n biallelic rows, each with one of the Alt alleles
   * in the altAllele column. The Ref allele in all these rows is the same as the Ref allele in the in the multiallelic
   * row.
   *
   * Each info field is appropriately split among split rows if it has the same number of elements as
   * number of Alt alleles, otherwise it is repeated in all split rows. A new info column called OLD_MULTIALLELIC
   * is added to the DataFrame, which for each split row, holds the contigName:position:Ref/Alt alleles of the
   * multiallelic row corresponding to the split row.
   *
   * Genotype fields are treated as follows: The calls (GT) field becomes biallelic in each row, where Alt alleles not
   * present in that row are replaced with no call ("."). The fields with number of entries equal to number of
   * (Ref+Alt) alleles, are properly split into rows, where in each split row, only entries corresponding to the Ref
   * allele as well as the Alt alleles present in that row are kept. The fields which follow colex order
   * (e.g., GL, PL, and GP) are properly split between split rows where in each row only the elements corresponding to
   * genotypes comprising of the Ref and Alt alleles in that row are listed. Other fields are just repeated over the
   * split rows.
   *
   * As an example (shown in VCF file format), the following multiallelic row
   *
   * #CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
   * 20	101	.	A	ACCA,TCGG	.	PASS	VC=INDEL;AC=3,2;AF=0.375,0.25;AN=8	GT:AD:DP:GQ:PL	0/1:2,15,31:30:99:2407,0,533,697,822,574
   *
   * will be split into the following two biallelic rows:
   *
   * #CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
   * 20	101	.	A	ACCA	.	PASS	VC=INDEL;AC=3;AF=0.375;AN=8;OLD_MULTIALLELIC=20:101:A/ACCA/TCGG	GT:AD:DP:GQ:PL	0/1:2,15:30:99:2407,0,533
   * 20	101	.	A	TCGG	.	PASS	VC=INDEL;AC=2;AF=0.25;AN=8;OLD_MULTIALLELIC=20:101:A/ACCA/TCGG	GT:AD:DP:GQ:PL	0/.:2,31:30:99:2407,697,574
   *
   * @param variantDf
   * @return dataframe of split variants
   */
  def splitVariants(
      variantDf: DataFrame,
      infoFieldsToSplit: Option[Seq[String]] = None): DataFrame = {

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
            col(startField.name) + 1,
            lit(":"),
            array_join(
              concat(array(col(refAlleleField.name)), col(alternateAllelesField.name)),
              "/")
          )
        ).otherwise(lit(null))
      )
      .select(
        col("*"),
        posexplode(col(alternateAllelesField.name))
          .as(Array(splitAlleleIdxFieldName, splitAllelesFieldName))
      )

    // Split INFO fields
    val dfAfterInfoSplit = splitInfoFields(dfAfterAltAlleleSplit, infoFieldsToSplit)

    // split genotypes fields, update alternateAlleles field, and drop the columns resulting from posexplode
    splitGenotypeFields(dfAfterInfoSplit)
      .withColumn(alternateAllelesField.name, array(col(splitAllelesFieldName)))
      .drop(splitAlleleIdxFieldName, splitAllelesFieldName)
  }

  /**
   * Generates a new DataFrame by splitting the info fields, based on splitAlleleIdx
   * field generated by posexplode
   *
   * @param variantDf
   * @return dataframe with split info fields
   */
  @VisibleForTesting
  private[splitmultiallelics] def splitInfoFields(
      variantDf: DataFrame,
      infoFieldsToSplit: Option[Seq[String]]): DataFrame = {
    val splittableFields = variantDf
      .schema
      .filter(field =>
        field.name.startsWith(infoFieldPrefix) && field.dataType.isInstanceOf[ArrayType] &&
        field.metadata.contains(VCFSchemaInferrer.VCF_HEADER_COUNT_KEY) &&
        field.metadata.getString(VCFSchemaInferrer.VCF_HEADER_COUNT_KEY) == VCFHeaderLineCount
          .A
          .toString)
      .map(_.name)

    val fieldsToSplit = infoFieldsToSplit.getOrElse(splittableFields)

    val columns = fieldsToSplit.map(field =>
      field ->
      when(
        col(splitFromMultiAllelicField.name) &&
        size(col(field)) === size(col(alternateAllelesField.name)),
        array(expr(s"${field}[$splitAlleleIdxFieldName]"))
      ).otherwise(col(field)))
    variantDf.withColumns(columns.toMap)
  }

  /**
   * Generates a new DataFrame by splitting the genotypes subfields, based on splitAlleleIdx
   * field generated by posexplode
   *
   * @param variantDf
   * @return dataframe with split genotype subfields
   */
  @VisibleForTesting
  private[splitmultiallelics] def splitGenotypeFields(variantDf: DataFrame): DataFrame = {

    val gSchema = InternalRowToVariantContextConverter.getGenotypeSchema(variantDf.schema)

    if (gSchema.isEmpty) {
      variantDf
    } else {
      // pull out genotypes subfields as new columns
      val extractedFields = gSchema
        .get
        .fields
        .map(field =>
          field.name ->
          when(
            col(splitFromMultiAllelicField.name),
            expr(s"transform(${genotypesFieldName}, g -> g.${field.name})")).otherwise(array()))
      val withExtractedFields = variantDf.withColumns(extractedFields.toMap)

      // update pulled-out genotypes columns, zip them back together as the new genotypes column,
      // and drop the pulled-out columns
      // Note: In performance tests, it was seen that nested transform sql functions used below work twice faster if
      // WholestageCodegen is off for spark sql. Therefore, it is recommended to set "spark.sql.codegen.wholeStage" conf
      // to off when using this splitter.

      val updatedColumns = gSchema
        .get
        .fields
        .collect {
          case f
              if structFieldsEqualExceptNullability(genotypeLikelihoodsField, f) |
              structFieldsEqualExceptNullability(phredLikelihoodsField, f) |
              structFieldsEqualExceptNullability(posteriorProbabilitiesField, f) =>
            // update genotypes subfields that have colex order using the udf
            f.name ->
            expr(s"""transform(${f.name}, c ->
                   |     filter(
                   |        transform(
                   |            c, (x, idx) ->
                   |              if (
                   |                  array_contains(
                   |                      transform(array_repeat(0, size(${callsField.name}[0]) + 1), (el, i) ->
                   |                        comb(size(${callsField.name}[0]) + $splitAlleleIdxFieldName + 1, size(${callsField.name}[0])) - comb(size(${callsField.name}[0]) + $splitAlleleIdxFieldName + 1 - i, size(${callsField.name}[0]) - i)
                   |                      ),
                   |                      idx
                   |                  ), x, null
                   |              )
                   |        ),
                   |        x -> !isnull(x)
                   |      )
                   |   )""".stripMargin)

          case f if structFieldsEqualExceptNullability(callsField, f) =>
            // update GT calls subfield
            f.name ->
            expr(
              s"transform(${f.name}, " +
              s"c -> transform(c, x -> if(x == 0, x, if(x == $splitAlleleIdxFieldName + 1, 1, -1))))"
            )

          case f if f.dataType.isInstanceOf[ArrayType] =>
            // update any ArrayType field with number of elements equal to number of alt alleles
            f.name ->
            expr(
              s"transform(${f.name}, c -> if(size(c) == size(${alternateAllelesField.name}) + 1," +
              s" array(c[0], c[$splitAlleleIdxFieldName + 1]), null))"
            )
        }

      withExtractedFields
        .withColumns(updatedColumns.toMap)
        .withColumn(
          genotypesFieldName,
          when(
            col(splitFromMultiAllelicField.name),
            arrays_zip(gSchema.get.fieldNames.map(col(_)): _*)).otherwise(col(genotypesFieldName)))
        .drop(gSchema.get.fieldNames: _*)
    }
  }

  @VisibleForTesting
  private[splitmultiallelics] val splitAlleleIdxFieldName = "splitAlleleIdx"

  @VisibleForTesting
  private[splitmultiallelics] val splitAllelesFieldName = "splitAlleles"

  private val oldMultiallelicFieldName = "OLD_MULTIALLELIC"

}
