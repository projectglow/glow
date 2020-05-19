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

package io.projectglow.transformers.blockvariantsandsamples

import io.projectglow.Glow
import io.projectglow.common.GlowLogging
import io.projectglow.sql.GlowBaseTest
import io.projectglow.common.VariantSchemas._
import io.projectglow.functions.genotype_states

import org.apache.spark.sql.functions._
import io.projectglow.transformers.blockvariantsandsamples.BlockVariantsAndSamplesTransformer._

import org.apache.spark.sql.types._

class BlockVariantsAndSamplesTransformerSuite extends GlowBaseTest with GlowLogging {

  lazy val sourceName: String = "vcf"
  lazy val testFolder: String = s"$testDataHome/variantsampleblockmaker-test"

  lazy val testVcf =
    s"$testDataHome/1000G.phase3.broad.withGenotypes.chr20.10100000.vcf"

  lazy val testExpectedTsv =
    s"$testFolder/1000G.phase3.broad.withGenotypes.chr20.10100000.100Samples.Blocked.tsv"

  def testBlockedvsExpected(
      originalVCFFileName: String,
      expectedBlockedFileName: String,
      variantsPerBlock: Int,
      sampleBlockCount: Int
  ): Unit = {

    val options: Map[String, String] = Map(
      VARIANTS_PER_BLOCK -> variantsPerBlock.toString,
      SAMPLE_BLOCK_COUNT -> sampleBlockCount.toString
    )

    val dfOriginal = spark
      .read
      .format(sourceName)
      .load(originalVCFFileName)
      .withColumn(
        valuesField.name,
        slice(
          genotype_states(
            col(genotypesFieldName)
          ),
          1,
          100
        ).cast(ArrayType(DoubleType))
      )

    val dfBlocked = Glow
      .transform(
        TRANSFORMER_NAME,
        dfOriginal,
        options
      )

    val dfExpected = spark
      .read
      .format("csv")
      .options(
        Map(
          "delimiter" -> "\t",
          "header" -> "true"
        )
      )
      .schema(
        StructType(
          Seq(
            headerField,
            sizeField,
            StructField(valuesField.name, StringType),
            headerBlockIdField,
            sampleBlockIdField,
            sortKeyField,
            meanField,
            stdDevField
          )
        )
      )
      .load(testExpectedTsv)
      .withColumn(
        valuesField.name,
        split(col(valuesField.name), ",").cast(ArrayType(DoubleType))
      )

    assert(dfBlocked.count() == dfExpected.count())

    dfExpected
      .collect
      .zip(
        dfBlocked.collect
      )
      .foreach {
        case (rowExp, rowBlocked) =>
          assert(rowExp.equals(rowBlocked), s"Expected\n$rowExp\nBlocked\n$rowBlocked")
      }
  }

  test("test blocked vs expected") {
    testBlockedvsExpected(
      testVcf,
      testExpectedTsv,
      20,
      7
    )
  }
}
