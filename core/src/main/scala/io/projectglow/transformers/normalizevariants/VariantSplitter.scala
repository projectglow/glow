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

import com.google.common.annotations.VisibleForTesting
import io.projectglow.common.GlowLogging
import io.projectglow.common.VariantSchemas._
import io.projectglow.vcf.InternalRowToVariantContextConverter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLUtils.wholestageCodegenBoundary
import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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

  @VisibleForTesting
  private[normalizevariants] def splitVariants(variantDf: DataFrame): DataFrame = {

    if (variantDf.schema.fieldNames.contains("attributes")) {
      // TODO: Unflattened INFO field splitting
      logger.warn(
        "The variant splitting transformer does not split unflattened INFO fields within the attributes column.")
    }

    // Update splitFromMultiAllelic column, add INFO_OLD_MULTIALLELIC column (see vt decompose)
    // and posexplode alternateAlleles column
    val dfAfterAltAlleleSplit = wholestageCodegenBoundary(variantDf)
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
            concat_ws("/", col(refAlleleField.name), col(alternateAllelesField.name))
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
   *
   * @param variantDf
   * @return dataframe with split genotype subfields
   */
  @VisibleForTesting
  private[normalizevariants] def splitGenotypeFields(variantDf: DataFrame): DataFrame = {

    val gSchema = InternalRowToVariantContextConverter.getGenotypeSchema(variantDf.schema)

    if (gSchema.isEmpty) {
      variantDf
    } else {
      // pull out genotypes subfields as new columns
      val withExtractedFields = gSchema
        .get
        .fields
        .foldLeft(variantDf)(
          (df, field) =>
            df.withColumn(
              field.name,
              expr(s"transform(${genotypesFieldName}, g -> g.${field.name})"))
        )
        .drop(genotypesFieldName)

      // register the udf that genotypes splitter uses
      withExtractedFields
        .sqlContext
        .udf
        .register(
          "likelihoodSplitUdf",
          (numAlleles: Int, ploidy: Int, alleleIdx: Int) =>
            refAltColexOrderIdxArray(numAlleles, ploidy, alleleIdx)
        )

      // update pulled-out genotypes columns, zip them back together as the new genotypes column,
      // and drop the pulled-out columns
      gSchema
        .get
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
                    expr(s"""transform(${f.name}, c ->
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

              case _ =>
                df
            }
        )
        .withColumn(genotypesFieldName, arrays_zip(gSchema.get.fieldNames.map(col(_)): _*))
        .drop(gSchema.get.fieldNames: _*)
    }

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

    val idxArray = new Array[Int](ploidy + 1)

    // generate vector of elements at positions p+1,p,...,2 on the altAlleleIdx'th diagonal of Pascal's triangle
    idxArray(0) = 0
    var i = 1
    idxArray(ploidy) = altAlleleIdx
    while (i < ploidy) {
      idxArray(ploidy - i) = idxArray(ploidy - i + 1) * (i + altAlleleIdx) / (i + 1)
      i += 1
    }

    // calculate the cumulative vector
    i = 1
    while (i <= ploidy) {
      idxArray(i) = idxArray(i) + idxArray(i - 1)
      i += 1
    }
    idxArray
  }

  @VisibleForTesting
  private[normalizevariants] val splitAlleleIdxFieldName = "splitAlleleIdx"

  @VisibleForTesting
  private[normalizevariants] val splitAllelesFieldName = "splitAlleles"

  @VisibleForTesting
  private[normalizevariants] val oldMultiallelicFieldName = "OLD_MULTIALLELIC"

}
