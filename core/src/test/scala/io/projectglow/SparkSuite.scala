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

package io.projectglow

import io.projectglow.SparkTestShim.FunSuite
import io.projectglow.sql.{GlowBaseTest, GlowTestData}
import org.apache.spark.sql.test.SharedSparkSession

class SparkSuite extends GlowBaseTest {
  test("spark program") {
    logger.warn("Testing env var " + System.getenv("SPARK_TESTING"))
    logger.warn("Testing java prop " + System.getProperty("spark.testing"))
    logger.warn("Spark Home" + System.getenv("SPARK_HOME"))
    logger.warn("Spark Java home" + System.getProperty("spark.test.home"))
    spark.range(10).count()
  }

  test("read file") {
    spark.read.text(s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf").count()
  }

  test("print vcf schema") {
    val schema =
      spark.read.format("vcf").load(s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf").schema
    schema.foreach { f =>
      print(s"$f, ${f.metadata}\n")
    }
  }
}
