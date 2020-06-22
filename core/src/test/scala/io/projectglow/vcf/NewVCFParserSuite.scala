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

package io.projectglow.vcf

import io.projectglow.sql.GlowBaseTest

class NewVCFParserSuite extends GlowBaseTest {
  test("new parser") {
    import org.apache.spark.sql.functions._
    val vcf = s"$testDataHome/1000G.phase3.broad.withGenotypes.chr20.10100000.vcf"
    val df = spark
      .read
      .format("vcf")
      .load(vcf)
      .withColumn("genotype", expr("genotypes[0]"))
//    val df = spark.read.format("vcf").load(vcf).selectExpr("expand_struct(genotypes[0])")
    df.printSchema()
    df.show()
  }

}
