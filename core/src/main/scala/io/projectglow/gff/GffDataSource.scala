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

package io.projectglow.gff

import io.projectglow.common.FeatureSchemas._
import io.projectglow.gff.GffDataSource._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.StringSplit
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, Filter, PrunedFilteredScan, RelationProvider, SchemaRelationProvider, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability


class GffDataSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister {

  override def shortName(): String = "gff"

  /**
   * Creates relation
   *
   * @param sqlContext spark sql context
   * @param parameters parameters for job
   * @return Base relation
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates relation with user schema
   *
   * @param sqlContext spark sql context
   * @param parameters parameters for job
   * @param schema     user defined schema
   * @return Base relation
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val path = parameters.get("path")
    path match {
      case Some(p) =>
        val requiredSchema = if (schema == null) {
          inferSchema (sqlContext, p)
        } else {
          schema
        }

        new GffResourceRelation(sqlContext, p, requiredSchema)
      case _ => throw new IllegalArgumentException("Path is required")
    }
  }

}


class GffResourceRelation(
  val sqlContext: SQLContext,
  path: String,
  requiredSchema: StructType
) extends BaseRelation with TableScan  { // TODO: investigate use of PrunedFilteredScan

  private val spark = sqlContext.sparkSession


  override def schema: StructType = {
    requiredSchema
  }


  override def buildScan(): RDD[Row] = {
    val csvDf = spark
      .read
      .option("sep", COLUMN_DELIMITER)
      .schema(GffDataSource.gffCsvSchema)
      .csv(path)

    val dfWithMappedAttributes = convertAttributesToMap(
      filterCommentsAndFasta(
        csvDf
      )
    )

    requiredSchema.foldLeft(dfWithMappedAttributes)((df, f) =>
      f match {
        case f if structFieldsEqualExceptNullability(f, startField) =>
          df.withColumn(
            f.name,
            col(f.name).cast(f.dataType) - 1
          )
        case f if structFieldsEqualExceptNullability(f, endField) =>
          df.withColumn(
            f.name,
            col(f.name).cast(f.dataType)
          )
        case f if structFieldsEqualExceptNullability(f, scoreField) |
          structFieldsEqualExceptNullability(f, strandField) |
          structFieldsEqualExceptNullability(f, phaseField) =>
          df.withColumn(
            f.name,
            when(
              col(f.name) =!= lit(NULL_IDENTIFIER),
              col(f.name).cast(f.dataType)
            ).otherwise(lit(null))
          )
        case f => // TODO: To be completed (also rectify column names)
          df.withColumn(
            f.name,
            element_at(col(attributesMapColumnName), f.name)
          )
      }
    )
      .drop(attributesField.name, attributesMapColumnName) // TODO: Complete the option of keeping attributes field
      .rdd
  }
}

object GffDataSource {

  def inferSchema(sqlContext: SQLContext, path: String): StructType = {
    val spark = sqlContext.sparkSession
    val csvDf = spark
      .read
      .option("sep", COLUMN_DELIMITER)
      .schema(GffDataSource.gffCsvSchema)
      .csv(path)


    val attributeTags = convertAttributesToMap(
      filterCommentsAndFasta(
        csvDf
      )
    )
      .select(attributesMapColumnName)
      .withColumn(
        attributesMapColumnName,
        explode(
          map_keys(
            col(attributesMapColumnName)
          )
        )
      )
      .agg(
        collect_set(
          attributesMapColumnName
        )
      )
      .collect()(0).getAs[Seq[String]](0)

    // generate the schema. The field names will be case and underscore insensitive
    val officialAttributeFields =
      gffOfficialAttributeFields.foldLeft(Seq[StructField]()) { (s, f) =>
        if (attributeTags
          .map(t => deUnderscore(t.toLowerCase))
          .contains(f.name.toLowerCase)) {
          s :+ f
        } else {
          s
        }
      }

    val remainingTags = attributeTags
      .filter(
        t =>
          !officialAttributeFields
            .map(_.name.toLowerCase)
            .contains(deUnderscore(t.toLowerCase)) && !t.isEmpty
      )

    val unofficialAttributeFields = remainingTags.foldLeft(Seq[StructField]()) { (s, t) =>
      s :+ StructField(t, StringType)
    }

    StructType(
      gffBaseSchema.fields ++ officialAttributeFields ++ unofficialAttributeFields
    )

  }

  def filterCommentsAndFasta(df: DataFrame): DataFrame = {
    df.where(
        !gffCsvSchema.fieldNames.drop(1).foldLeft(lit(true))(
          (f1, f2) =>
            f1 && isnull(col(f2))
        ) &&
          substring(col(seqIdField.name), 1, 1) =!= lit(COMMENT_IDENTIFIER) &&
          substring(col(seqIdField.name), 1, 1) =!= lit(CONTIG_IDENTIFIER)
      )
  }

  def convertAttributesToMap(df: DataFrame): DataFrame = {
    df.withColumn(
        attributesMapColumnName,
        expr(
          s"""str_to_map(${attributesField.name},
             | "$ATTRIBUTES_DELIMITER",
             |  "$GFF3_TAG_VALUE_DELIMITER")""".stripMargin
        )
      )
  }



  def deUnderscore(s: String): String = {
    s.replaceAll("_", "")
  }

  private[gff] val gffCsvSchema = StructType(
    gffBaseSchema
      .fields
      .toSeq
      .map(f => StructField(f.name, StringType))
      :+ attributesField
  )

  private[gff] val attributesMapColumnName = "attributesMap"

  private[gff] val COLUMN_DELIMITER = "\t"
  private[gff] val ATTRIBUTES_DELIMITER = ";"
  private[gff] val GFF3_TAG_VALUE_DELIMITER = "="
  private[gff] val GTF_TAG_VALUE_DELIMITER = " "
  private[gff] val COMMENT_IDENTIFIER = "#"
  private[gff] val CONTIG_IDENTIFIER = ">"
  private[gff] val FASTA_LINE_REGEX = "(?i)[acgt]*"
  private[gff] val NULL_IDENTIFIER = "."
  private[gff] val ARRAY_DELIMITER = ","

}
