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
import io.projectglow.gff.GffFileFormat._
import io.projectglow.sql.GlowBaseTest

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class GffReaderSuite extends GlowBaseTest {
  lazy val testRoot = s"$testDataHome/gff"
  lazy val testGff3 = s"$testRoot/testgffAttWithFasta.gff"

  private val sourceName = "gff"

  // TODO: Add tests

  test("schema") {
    val unofficialFields = Seq(
      StructField("description", StringType),
      StructField("gene_biotype", StringType),
      StructField("gene_synonym", StringType),
      StructField("chromosome", StringType),
      StructField("transcript_id", StringType),
      StructField("gbkey", StringType),
      StructField("genome", StringType),
      StructField("mol_type", StringType),
      StructField("gene", StringType),
      StructField("pseudo", StringType),
      StructField("product", StringType)
    )

    val expectedSchema = StructType(
      gffBaseSchema.fields.toSeq ++
      Seq(idField, nameField, parentField, dbxrefField, isCircularField) ++
      unofficialFields
    )
    val df = spark
      .read
      .format(sourceName)
      .load(testGff3)

    assert(df.schema.equals(expectedSchema))
  }

  test("Read gff3") {
    val dfRow = spark
      .read
      .format(sourceName)
      .load(s"$testRoot/testgffAttWithFasta.gff")
      .orderBy("seqId", "start")
      .take(1)(0)

    val expectedRow = Row(
      "NC_000001.11",
      "RefSeq",
      "region",
      0,
      248956422,
      null,
      "+",
      1,
      "NC_000001.11:1..248956422",
      "1",
      null,
      Seq("taxon:9606", "test"),
      false,
      null,
      null,
      null,
      "1",
      null,
      "Src",
      "chromosome",
      "genomic DNA",
      null,
      null,
      null
    )

    assert(dfRow == expectedRow)
  }

  test("updateAttFieldsWithParsedTags") {
    val currentToken = ParsedAttributesToken(None, Set(idField.name, nameField.name))
    val attributes = s"${idField.name}=1234;${aliasField.name}=monkey"
    val expected = ParsedAttributesToken(
      Some(GFF3_TAG_VALUE_DELIMITER),
      Set(idField.name, nameField.name, aliasField.name))
    assert(updateAttributesToken(currentToken, attributes) == expected)
  }

  test("GFF file format does not support writing") {
    val df = spark
      .read
      .format(sourceName)
      .load(testGff3)
    val e = intercept[UnsupportedOperationException] {
      df.write.format(sourceName).save("noop")
    }
    assert(e.getMessage.contains("GFF data source does not currently support writing."))
  }

  test("gff") {
    val df = spark.read
    // .schema(StructType(gffBaseSchema.fields :+ attributesField))
      .format(sourceName)
      .load(s"$testRoot/testgffAttWithFasta.gff")
    // .load(s"$testRoot/GCF_000001405.39_GRCh38.p13_genomic.gff.bgz")

    df.show()
  }
}
