package org.projectglow

import org.apache.spark.sql.DataFrame

import org.projectglow.sql.GlowBaseTest

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
