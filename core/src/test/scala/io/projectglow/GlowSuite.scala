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

class GlowSuite extends GlowBaseTest {
  test("uses service provider") {
    val sess = spark
    import sess.implicits._
    val output =
      Glow.transform("dummy_transformer", spark.emptyDataFrame, Map.empty[String, String])
    assert(output.count() == 1)
    assert(output.as[String].head() == "monkey")
  }

  test("transformer names are converted to snake case") {
    val sess = spark
    import sess.implicits._
    val output =
      Glow.transform("dummyTransformer", spark.emptyDataFrame, Map.empty[String, String])
    assert(output.count() == 1)
    assert(output.as[String].head() == "monkey")
  }
}

class DummyTransformer extends DataFrameTransformer {
  override def name: String = "dummy_transformer"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {
    df.sparkSession.createDataFrame(Seq(StringWrapper("monkey")))
  }
}

case class StringWrapper(s: String)
