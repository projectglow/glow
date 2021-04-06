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

import org.apache.spark.sql.DataFrame
import io.projectglow.sql.GlowBaseTest
import org.apache.spark.SparkException

class GlowSuite extends GlowBaseTest {
  def checkTransform(df: DataFrame): Unit = {
    val sess = spark
    import sess.implicits._
    assert(df.count() == 2)
    assert(df.as[String].collect.toSeq == Seq("camel", "snake"))
  }

  test("uses service provider") {
    val df =
      Glow.transform(
        "dummy_transformer",
        spark.emptyDataFrame,
        Map("camel_animal" -> "camel", "snake_animal" -> "snake"))
    checkTransform(df)
  }

  test("transformer names are converted to snake case") {
    val df =
      Glow.transform(
        "dummyTransformer",
        spark.emptyDataFrame,
        Map("camel_animal" -> "camel", "snake_animal" -> "snake"))
    checkTransform(df)
  }

  test("options are converted to snake case") {
    val df =
      Glow.transform(
        "dummyTransformer",
        spark.emptyDataFrame,
        Map("camelAnimal" -> "camel", "snake_animal" -> "snake"))
    checkTransform(df)
  }

  test("java map options") {
    val javaMap = new java.util.HashMap[String, String]
    javaMap.put("camelAnimal", "camel")
    javaMap.put("snake_animal", "snake")
    val df = Glow.transform("dummyTransformer", spark.emptyDataFrame, javaMap)
    checkTransform(df)
  }

  test("tuple options") {
    val df =
      Glow.transform(
        "dummyTransformer",
        spark.emptyDataFrame,
        ("camelAnimal", "camel"),
        ("snake_animal", "snake"))
    checkTransform(df)
  }

  test("accept non-string values") {
    intercept[IllegalArgumentException] {
      Glow.transform("dummyTransformer", spark.emptyDataFrame, Map("must_be_true" -> false))
    }
    Glow.transform("dummyTransformer", spark.emptyDataFrame, Map("must_be_true" -> true))
  }

  test("float arguments") {
    intercept[IllegalArgumentException] {
      Glow.transform("dummyTransformer", spark.emptyDataFrame, Map("pi" -> 15.48))
    }
    Glow.transform("dummyTransformer", spark.emptyDataFrame, Map("pi" -> 3.14159))
    Glow.transform("dummyTransformer", spark.emptyDataFrame, Map("pi" -> "3.14159"))
  }

  test("registers bgz conf") {
    val sess = spark.newSession()
    val path = s"$testDataHome/vcf-merge/HG00096.vcf.bgz"

    intercept[SparkException] {
      // Exception because bgz codec is not registered
      sess.read.format("vcf").load(path).collect()
    }

    val sessWithGlow = Glow.register(sess)
    sessWithGlow.read.format("vcf").load(path).collect() // No error
  }
}

class DummyTransformer extends DataFrameTransformer {
  override def name: String = "dummy_transformer"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {
    val animals = Seq(options.get("camel_animal"), options.get("snake_animal")).flatten
    if (!options.get("must_be_true").forall(_.toBoolean)) {
      throw new IllegalArgumentException("if provided, this arg must be true")
    }

    options.get("pi").foreach { pi =>
      require(Math.abs(pi.toDouble - Math.PI) < Math.PI * 0.0001)
    }

    df.sparkSession.createDataFrame(animals.map(StringWrapper)).sort()
  }
}

case class StringWrapper(s: String)
