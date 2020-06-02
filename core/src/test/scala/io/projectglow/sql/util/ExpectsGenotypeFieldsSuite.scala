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

package io.projectglow.sql.util

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions._

import io.projectglow.Glow
import io.projectglow.functions._
import io.projectglow.sql.GlowBaseTest

class ExpectsGenotypeFieldsSuite extends GlowBaseTest {
  lazy val gatkTestVcf =
    s"$testDataHome/variantsplitternormalizer-test/test_left_align_hg38_altered.vcf"
  lazy val sess = spark

  // This is how we originally detected an issue where ExpectsGenotypeFields succeeds during
  // resolution but fails during physical planning.
  // PR: https://github.com/projectglow/glow/pull/224
  test("use genotype_states after splitting multiallelics") {
    val df = spark.read.format("vcf").load(gatkTestVcf)
    val split = Glow.transform("split_multiallelics", df)
    split.select(genotype_states(col("genotypes"))).collect()
  }

  test("use genotype_states after array_zip") {
    import sess.implicits._
    val df = spark
      .createDataFrame(Seq((Seq("a"), Seq(Seq(1, 1)))))
      .withColumnRenamed("_1", "sampleId")
      .withColumnRenamed("_2", "calls")
    val zipped = df.select(arrays_zip(col("sampleId"), col("calls")).as("genotypes"))
    val states = zipped.select(genotype_states(col("genotypes")))
    assert(states.as[Seq[Int]].head == Seq(2))
  }

  test("type check") {
    val df = spark.createDataFrame(Seq(Tuple1("a"))).withColumnRenamed("_1", "sampleId")
    val withGenotypes = df.select(array(struct("sampleId")).as("genotypes"))
    val ex = intercept[AnalysisException](withGenotypes.select(genotype_states(col("genotypes"))))
    assert(ex.message.contains("Genotype struct was missing required fields: (name: calls"))
  }
}
