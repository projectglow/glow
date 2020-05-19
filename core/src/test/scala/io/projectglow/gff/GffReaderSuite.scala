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
import io.projectglow.sql.GlowBaseTest

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class GffReaderSuite extends GlowBaseTest {
  lazy val testRoot = s"$testDataHome/gff"
  lazy val testGff3 = s"$testRoot/test_gff_with_fasta.gff"
  lazy val testGff3Gzip = s"$testRoot/test_gff_with_fasta.gff.gz"
  lazy val testGff3Bgzip = s"$testRoot/test_gff_with_fasta.gff.bgz"
  lazy val testGff3BgzipWithGzSuffix = s"$testRoot/test_gff_with_fasta_bgzip.gff.gz"
  lazy val testGff3Empty = s"$testRoot/test_gff_empty.gff"

  lazy val testGff3MultiCaseAttribute = s"$testRoot/test_gff_with_fasta_multicase_attribute.gff"

  private val sourceName = "gff"

  private val testOfficialFields = Seq(
    StructField("ID", StringType),
    StructField("Name", StringType),
    StructField("Parent", ArrayType(StringType)),
    StructField("Dbxref", ArrayType(StringType)),
    StructField("Is_circular", BooleanType)
  )

  private val testUnofficialFields = Seq(
    StructField("chromosome", StringType),
    StructField("description", StringType),
    StructField("gbkey", StringType),
    StructField("gene", StringType),
    StructField("gene_biotype", StringType),
    StructField("gene_synonym", StringType),
    StructField("genome", StringType),
    StructField("mol_type", StringType),
    StructField("product", StringType),
    StructField("pseudo", StringType),
    StructField("test space", StringType),
    StructField("transcript_id", StringType)
  )

  private val testSchemaAllSmall = StructType(
    gffBaseSchema.fields :+
    StructField("dbxref", ArrayType(StringType))
    :+ StructField("iscircular", BooleanType)
  )

  private val testSchemaMixedCase = StructType(
    gffBaseSchema.fields :+
    StructField("Dbxref", ArrayType(StringType))
    :+ StructField("isCircular", BooleanType)
  )

  private val testSchemaWithUnderscore = StructType(
    gffBaseSchema.fields :+
    StructField("dbx_Ref", ArrayType(StringType))
    :+ StructField("is_Circular", BooleanType)
  )

  private val testSchemaExpected = StructType(
    gffBaseSchema.fields :+
    StructField("Dbxref", ArrayType(StringType))
    :+ StructField("Is_circular", BooleanType)
  )

  gridTest("Schema inference")(
    Seq(
      testGff3,
      // test if schema is inferred correctly when the attribute tag is not consistent across the rows
      // in terms of being lower or upper case (Name tag has different cases across the rows of this
      // test file.)
      testGff3MultiCaseAttribute
    )
  ) { file =>
    val df = spark
      .read
      .format(sourceName)
      .load(file)

    val expectedSchema = StructType(
      gffBaseSchema.fields.dropRight(1) ++ testOfficialFields ++ testUnofficialFields
    )

    assert(df.schema.equals(expectedSchema))
  }

  gridTest("Case-and-underscore-insensitive attribute column names in user-specified schema")(
    Seq(testSchemaAllSmall, testSchemaMixedCase, testSchemaWithUnderscore)
  ) { s =>
    val dfRow = spark
      .read
      .format(sourceName)
      .schema(s)
      .load(testGff3)
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
      "ID=NC_000001.11:1..248956422;Dbxref=taxon:9606,test;Name=1;chromosome=1;" +
      "gbkey=Src;genome=chromosome;mol_type=genomic DNA;Is_circular=false",
      Array("taxon:9606", "test").toSeq,
      false
    )
    assert(dfRow == expectedRow)
  }

  test("Read gff with user-specified schema containing attributesField") {
    val schemaWithAttributesField = StructType(
      gffBaseSchema.fields ++ testOfficialFields ++ testUnofficialFields
    )

    val dfRow = spark
      .read
      .schema(schemaWithAttributesField)
      .format(sourceName)
      .load(testGff3)
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
      "ID=NC_000001.11:1..248956422;Dbxref=taxon:9606,test;Name=1;chromosome=1;" +
      "gbkey=Src;genome=chromosome;mol_type=genomic DNA;Is_circular=false",
      "NC_000001.11:1..248956422",
      "1",
      null,
      Array("taxon:9606", "test").toSeq,
      false,
      "1",
      null,
      "Src",
      null,
      null,
      null,
      "chromosome",
      "genomic DNA",
      null,
      null,
      null,
      null
    )
    assert(dfRow == expectedRow)
  }

  test("Read gff with user-specified schema containing nonexistent column") {
    val schemaWithAttributesField = StructType(
      gffBaseSchema.fields ++ testOfficialFields :+ StructField("nonexistent", StringType)
    )
    // must return an all-null column
    assert(
      spark
        .read
        .schema(schemaWithAttributesField)
        .format(sourceName)
        .load(testGff3)
        .collect()
        .forall(_(14) == null)
    )
  }

  gridTest("Read gff3, gzipped gff3 and bgzipped gff3 with inferred schema")(
    Seq(testGff3, testGff3Gzip, testGff3Bgzip, testGff3BgzipWithGzSuffix)
  ) { file =>
    val dfRows = spark
      .read
      .format(sourceName)
      .load(file)
      .orderBy("seqId", "start", "source")
      .take(2)

    val expectedRows = Array(
      Row(
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
        Array("taxon:9606", "test").toSeq,
        false,
        "1",
        null,
        "Src",
        null,
        null,
        null,
        "chromosome",
        "genomic DNA",
        null,
        null,
        null,
        null
      ),
      Row(
        "NC_000001.11",
        "BestRefSeq",
        "pseudogene",
        11873,
        14409,
        null,
        null,
        null,
        "gene-DDX11L1",
        "DDX11L1",
        null,
        Array("GeneID:100287102", "HGNC:HGNC:37102").toSeq,
        null,
        null,
        "DEAD/H-box helicase 11 like 1 (pseudogene)",
        "Gene",
        "DDX11L1",
        "transcribed_pseudogene",
        null,
        null,
        null,
        null,
        "true",
        "passed",
        null
      )
    )
    assert(dfRows.sameElements(expectedRows))
  }

  test("Column pruning") {
    val dfRows = spark
      .read
      .format(sourceName)
      .load(testGff3)
      .select(
        "seqId",
        "source",
        "start",
        "end",
        "Dbxref",
        "Is_circular",
        "gbkey",
        "test space"
      )
      .orderBy("seqId", "start", "source")
      .take(2)

    val expectedRows = Array(
      Row(
        "NC_000001.11",
        "RefSeq",
        0,
        248956422,
        Array("taxon:9606", "test").toSeq,
        false,
        "Src",
        null
      ),
      Row(
        "NC_000001.11",
        "BestRefSeq",
        11873,
        14409,
        Array("GeneID:100287102", "HGNC:HGNC:37102").toSeq,
        null,
        "Gene",
        "passed"
      )
    )
    assert(dfRows.sameElements(expectedRows))
  }

  // See the comment in io.projectglow.gff.GffResourceRelation.buildScan on use of filterFastaLines
  test("df.count vs df.rdd.count") {
    val df = spark
      .read
      .format(sourceName)
      .load(testGff3)
    assert(df.count() == 20)
    assert(df.count() == df.rdd.count())
  }

  test("Read gff glob") {
    val df = spark
      .read
      .format(sourceName)
      .load(s"$testRoot/*")

    assert(df.count() == 100)
    assert(df.filter("start == 0").count() == 5)
  }

  test("Read empty gff") {
    val df = spark
      .read
      .format(sourceName)
      .load(testGff3Empty)

    assert(df.schema == StructType(gffBaseSchema.fields.dropRight(1)))
    assert(df.count() == 0) // requiredColumns will be empty
    assert(df.rdd.count() == 0) // requiredColumns will be nonEmpty

  }

  test("GFF file format does not support writing") {
    val df = spark
      .read
      .format(sourceName)
      .load(testGff3)
    val e = intercept[UnsupportedOperationException] {
      df.write.format(sourceName).save("noop")
    }
    assert(e.getMessage.contains("GFF data source does not support writing!"))
  }
}
