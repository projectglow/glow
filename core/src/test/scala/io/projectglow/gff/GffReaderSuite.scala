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

import java.io.{EOFException, FileNotFoundException}

import io.projectglow.common.FeatureSchemas._
import io.projectglow.gff.GffFileFormat._
import io.projectglow.sql.GlowBaseTest

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType
import org.apache.spark.{DebugFilesystem, SparkException}

class GffReaderSuite extends GlowBaseTest {
  private val testRoot = s"$testDataHome/gff"
  private val sourceName = "gff"

  test("gff") {
    val df = spark
      .read
      // .schema(gffBaseSchema)
      .format(sourceName)
      .load(s"$testRoot/testgffAttWithFasta.gff")
      //.load(s"$testRoot/GCF_000001405.39_GRCh38.p13_genomic.gff.bgzf")

    df.show()
  }

  test("updateAttFieldsWithParsedTags") {
    val currentToken = ParsedAttributesToken(Some('='), Set(idField.name, nameField.name))
    val attributes = Array(s"${idField.name}=1234", s"${aliasField.name}=monkey")
    val expected = Set(idField.name, nameField.name, aliasField.name)
    assert(updateAttributesToken(currentToken, attributes) == expected)
  }


}
