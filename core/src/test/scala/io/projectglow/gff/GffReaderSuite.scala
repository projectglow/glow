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

import java.text.Normalizer

import io.projectglow.common.FeatureSchemas._
import io.projectglow.gff.GffDataSource._
import io.projectglow.sql.GlowBaseTest

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, BooleanType, StringType, StructField, StructType}

class GffReaderSuite extends GlowBaseTest {
  lazy val testRoot = s"$testDataHome/gff"
  lazy val testGff3 = s"$testRoot/test_gff_with_fasta.gff"
  lazy val testGff3Gzip = s"$testRoot/test_gff_with_fasta_gzip.gff.gz"
  lazy val testGff3Bgzip = s"$testRoot/test_gff_with_fasta_bgzip.gff.bgz"
  lazy val testGff3BgzipWithGzSuffix = s"$testRoot/test_gff_with_fasta_bgzip.gff.bgz"
  lazy val testGffEmpty = s"$testRoot/test_gff_empty.gff"

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


  gridTest("Read gff3, gzipped gff3 and bgzipped gff3 with inferred schema")(
    Seq(testGff3, testGff3Gzip, testGff3Bgzip)
  ) { file =>
    val dfRow = spark
      .read
      .format(sourceName)
      .load(file)
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

  test("Read gff with user specified schema containing attributesField") {
    val dfRow = spark.read
      .schema(StructType(gffBaseSchema.fields :+ attributesField))
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
      "ID=NC_000001.11:1..248956422;Dbxref=taxon:9606,test;Name=1;chromosome=1;gbkey=Src;genome=chromosome;mol_type=genomic DNA;Is_circular=false"
    )
    assert(dfRow == expectedRow)
  }

  /* test("GFF file format does not support writing") {
    val df = spark
      .read
      .format(sourceName)
      .load(testGff3)
    val e = intercept[UnsupportedOperationException] {
      df.write.format(sourceName).save("noop")
    }
    assert(e.getMessage.contains("GFF data source does not currently support writing."))
  } */

  test("Read gff glob") {
    val df = spark
      .read
      .format(sourceName)
      .load(s"$testRoot/*")

    assert(df.count() == 60)
    assert(df.filter("start == 0").count() == 3)
  }

  test("Read empty gff") {
    val df = spark.read
      .format(sourceName)
      .load(testGffEmpty)

    assert(df.count() == 0)
    assert(df.schema == gffBaseSchema)
  }

  test("Read gff with user specified schema containing attributes subfields") {
    val df = spark.read
      .schema(
        StructType(
          gffBaseSchema.fields ++ Seq(
            idField,
            isCircularField,
            dbxrefField,
            StructField("gene", StringType)
          )
        )
      ).format(sourceName)
      .load(testGff3)

    df.show()
  }

  test("test gff") {
    val testSchema = StructType(
      Seq(
        seqIdField,
        sourceField,
        typeField,
        startField,
        endField,
        StructField("score", StringType),
        strandField,
        StructField("phase", StringType),
        attributesField
      )
    )
    val df = spark.read
      // .schema(testSchema)
      .schema(
      StructType(
        gffBaseSchema.fields :+
          StructField("Dbx_ref", StringType) :+
          StructField("iscircular", StringType)

      )
    )
      .format(sourceName)
      .load(testGff3)
    //  .select(seqIdField.name)

    df.printSchema()

    df.show()
  }

}
