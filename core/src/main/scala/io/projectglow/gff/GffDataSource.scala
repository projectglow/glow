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
import org.apache.spark.sql.SQLUtils.{dataTypesEqualExceptNullability, structFieldsEqualExceptNullability}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Gff data source to read GFF3 files.
 *
 * The data source is able to infer the schema or accept a user-specified schema. It flattens the
 * attributes field by creating a column for each tag that appears in the attributes column of
 * the gff file.
 *
 * The inferred schema will have base fields corresponding to the first 8 columns of gff3 called
 * seqId, source, type, start, end, score, strand, and phase, followed by any official
 * attribute field among id, name, alias, parent, target, gap, derivesfrom, note, dbxref,
 * ontologyterm, and iscircular that appears in the gff tags followed by any unofficial attribute
 * field that appears in the tags. In the inferred schema, the base and official fields will be in
 * the same order as listed above. The unofficial fields will be in alphabetical order.
 *
 * Any user-specified schema can have any subset of fields corresponding to the 9 columns of gff3
 * (named seqId, source, type, start, end, score, strand, phase, and attributes), the
 * official attribute fields, and the unofficial attribute fields. The name of the official and
 * unofficial fields should match the tag name in a case-and-underscore-insensitive fashion.
 */
class GffDataSource
    extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider
    with DataSourceRegister {

  override def shortName(): String = "gff"

  /**
   * Creates relation
   *
   * @param sqlContext spark sql context
   * @param parameters parameters for job
   * @return Base relation
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val inferredSchema = inferSchema(sqlContext, checkAndGetPath(parameters))
    createRelation(sqlContext, parameters, inferredSchema)
  }

  /**
   * Creates relation with user schema. User-specified schema can have a subset of attribute columns
   * as they will be parsed out of "attributes" column
   *
   * @param sqlContext spark sql context
   * @param parameters parameters for job
   * @param schema     user defined schema
   * @return Base relation
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    new GffResourceRelation(sqlContext, checkAndGetPath(parameters), schema)
  }

  /**
   * Saves a DataFrame to a destination (not supported in this DataSource.)
   */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    throw new UnsupportedOperationException(
      "GFF data source does not support writing!"
    )
  }
}

class GffResourceRelation(
    val sqlContext: SQLContext,
    path: String,
    requiredSchema: StructType
) extends BaseRelation
    with PrunedScan {

  private val spark = sqlContext.sparkSession

  // To be able to use RDD[InternalRow] asInstanceOf RDD[Row] in buildScan
  override def needConversion: Boolean = false

  override def schema: StructType = {
    requiredSchema
  }

  /**
   * buildScan for a BaseRelation that can eliminate unneeded columns before producing an RDD.
   * It uses csv data source to read the gff file. In addition to base columns, it parses
   * "attributes" column to populate the flattened attribute columns in the schema in correct
   * data type. If schema contains "attributes" columns the original attributes
   * column will be included as well.
   *
   * @param requiredColumns
   * @return: generated RDD[Rows]
   */
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {

    val originalColumnPruning: Boolean =
      spark.conf.get(columnPruningConf).toBoolean

    // csv column pruning is turned off temporarily for the malformed rows to/ get filtered
    // by DROPMALFORMED option of csv reader no matter which columns are selected
    // in the query. See https://issues.apache.org/jira/browse/SPARK-28058
    if (originalColumnPruning) {
      spark.conf.set(columnPruningConf, false)
    }

    val csvDf = spark
      .read
      .options(csvReadOptions)
      .schema(gffBaseSchema)
      .csv(path)

    val attributeColumnNamesInSchema = schema
      .fieldNames
      .filter(!gffBaseSchema.fieldNames.contains(_))

    val dfWithMappedAttributes = normalizeAttributesMapKeys(
      addAttributesMapColumn(
        // Although DROPMALFORMED option is used, due to a bug in Spark 2.4.4 and before
        // (see https://issues.apache.org/jira/browse/SPARK-29101), the DataFrame.count() function
        // still includes malformed rows in the count. Therefore, for the count function to work
        // correctly on DataFrames created by gff data source we need to explicitly filter fasta lines.
        // TODO: Remove this after upgrade to Spark 2.4.5 and above
        filterFastaLines(csvDf)
      )
    )

    val finalRdd = schema
      .foldLeft(dfWithMappedAttributes)((df, f) =>
        f match {
          case f if structFieldsEqualExceptNullability(f, startField) =>
            df.withColumn(
              f.name,
              col(f.name) - 1
            )

          // Correspond each column name to a tag in the map and extract the value to populate the
          // column and cast it into the correct data type. The correspondence between tags and column
          // names is case-and-underscore-insensitive.
          case f if attributeColumnNamesInSchema.contains(f.name) =>
            df.withColumn(
              f.name,
              if (dataTypesEqualExceptNullability(f.dataType, ArrayType(StringType))) {
                split(
                  element_at(col(attributesMapColumnName), normalizeString(f.name)),
                  ARRAY_DELIMITER
                )
              } else {
                element_at(col(attributesMapColumnName), normalizeString(f.name))
                  .cast(f.dataType)
              }
            )

          case _ =>
            df
        })
      .drop(attributesMapColumnName)
      .select(requiredColumns.map(col(_)): _*)
      .queryExecution
      .toRdd
      .asInstanceOf[RDD[Row]]

    spark.conf.set(columnPruningConf, originalColumnPruning)

    finalRdd
  }
}

object GffDataSource {

  private[gff] val attributesMapColumnName = "attributesMap"

  private[gff] val COLUMN_DELIMITER = "\t"
  private[gff] val ATTRIBUTES_DELIMITER = ";"
  private[gff] val GFF3_TAG_VALUE_DELIMITER = "="
  private[gff] val GTF_TAG_VALUE_DELIMITER = " "
  private[gff] val COMMENT_IDENTIFIER = "#"
  private[gff] val NULL_IDENTIFIER = "."
  private[gff] val ARRAY_DELIMITER = ","

  private[gff] val csvReadOptions = Map(
    "sep" -> COLUMN_DELIMITER,
    "comment" -> COMMENT_IDENTIFIER,
    "mode" -> "DROPMALFORMED",
    "nullValue" -> NULL_IDENTIFIER
  )

  private[gff] val columnPruningConf = "spark.sql.csv.parser.columnPruning.enabled"

  def checkAndGetPath(options: Map[String, String]): String = {
    options.get("path") match {
      case Some(p) => p
      case _ =>
        throw new IllegalArgumentException("Path is required")
    }
  }

  /**
   * Infers the schema by reading the gff file using csv data source and parsing the attributes fields
   * to get official and unofficial attribute columns. Drops the original "attributes" column. Names,
   * types and ordering of columns will be as follows:
   * Names: All attribute fields will have names exactly as in tags.
   * Types: Official attribute fields will have the types in
   *       [[io.projectglow.common.FeatureSchemas.gffOfficialAttributeFields]]. Detection of
   *       official field names among tags are case-and-underscore-insensitive. Unofficial
   *       attribute fields will have StringType.
   * Ordering: gffBaseSchema fields come first, followed by official attributes fields as ordered
   *           in [[io.projectglow.common.FeatureSchemas.gffOfficialAttributeFields]], followed by
   *           unofficial attribute fields in case-insensitive alphabetical order.
   *
   * @param sqlContext
   * @param path: path to gff file or glob
   * @return inferred schema
   */
  def inferSchema(sqlContext: SQLContext, path: String): StructType = {
    val spark = sqlContext.sparkSession

    val originalColumnPruning: Boolean =
      spark.conf.get(columnPruningConf).toBoolean

    // csv column pruning is turned off temporarily for the malformed rows (such as Fasta rows) to
    // get filtered by DROPMALFORMED mode option of csv reader no matter which columns are selected
    // in the query. See https://issues.apache.org/jira/browse/SPARK-28058
    if (originalColumnPruning) {
      spark.conf.set(columnPruningConf, false)
    }

    val csvDf = spark
      .read
      .options(csvReadOptions)
      .schema(gffBaseSchema)
      .csv(path)

    val attributeTags = addAttributesMapColumn(csvDf)
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
      .collect()(0)
      .getAs[Seq[String]](0)
      .filter(!_.isEmpty)
      .groupBy(_.toLowerCase)
      .mapValues(_.head)
      .values
      .to[collection.immutable.Seq] // toSeq does not work

    spark.conf.set(columnPruningConf, originalColumnPruning)

    val attributeFields = attributeTags
      .map(
        t =>
          StructField(
            t,
            gffOfficialAttributeFields
              .find(f => f.name == normalizeString(t))
              .map(_.dataType)
              .getOrElse(StringType)))
      .sortBy(f =>
        (gffOfficialAttributeFields.map(_.name).indexOf(normalizeString(f.name)) match {
          case -1 => gffOfficialAttributeFields.length + 1
          case i => i
        }, f.name))

    StructType(
      gffBaseSchema
        .fields
        .dropRight(1) // dropping attributes which is the last field in gffBaseSchema
      ++ attributeFields
    )
  }

  def addAttributesMapColumn(df: DataFrame): DataFrame = {
    df.withColumn(
      attributesMapColumnName,
      expr(
        s"""str_to_map(
           |       ${attributesField.name},
           |       "$ATTRIBUTES_DELIMITER",
           |       "$GFF3_TAG_VALUE_DELIMITER"
           |   )""".stripMargin
      )
    )
  }

  def normalizeAttributesMapKeys(df: DataFrame): DataFrame = {
    df.withColumn(
      attributesMapColumnName,
      map_from_arrays(
        expr(
          s"""transform(
            |       map_keys($attributesMapColumnName),
            |       k -> regexp_replace(lower(k), "_", "")
            |   )""".stripMargin
        ),
        map_values(col(attributesMapColumnName))
      )
    )
  }

  def filterFastaLines(df: DataFrame): DataFrame = {
    df.where(
      !isnull(
        coalesce(
          gffBaseSchema.fieldNames.drop(1).map(col(_)): _*
        )
      )
    )
  }

  def normalizeString(s: String): String = {
    s.toLowerCase.replaceAll("_", "")
  }
}
