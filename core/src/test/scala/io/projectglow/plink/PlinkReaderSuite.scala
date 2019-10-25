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

package io.projectglow.plink

import io.projectglow.common.{PlinkGenotype, PlinkRow}
import io.projectglow.sql.GlowBaseTest

class PlinkReaderSuite extends GlowBaseTest {
  private val testBed = s"$testDataHome/plink/small-test.bed"

  test("Read PLINK files") {
    val sess = spark
    import sess.implicits._

    val plinkRows = spark
      .read
      .format("plink")
      .option("bim_delimiter", "\t")
      .load(testBed)
      .sort("contigName")
      .as[PlinkRow]
      .collect
    plinkRows.foreach(println)

    assert(plinkRows.length == 5)

    val snp1 = plinkRows.head
    assert(
      snp1 == PlinkRow(
        contigName = "1",
        position = 0.0,
        start = 999,
        end = 1000,
        names = Seq("snp1"),
        referenceAllele = "A",
        alternateAlleles = Seq("C"),
        genotypes = Seq(
          PlinkGenotype(
            sampleId = "1",
            calls = Seq(0, 0)
          ),
          PlinkGenotype(
            sampleId = "2",
            calls = Seq(0, 1)
          ),
          PlinkGenotype(
            sampleId = "3",
            calls = Seq(1, 1)
          ),
          PlinkGenotype(
            sampleId = "4",
            calls = Seq(-1, -1)
          )
        )
      ))
  }
}
