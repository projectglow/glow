package com.databricks.hls

import org.apache.spark.sql.DataFrame

import com.databricks.hls.sql.HLSBaseTest

class DBGenomicsSuite extends HLSBaseTest {
  test("uses service provider") {
    val sess = spark
    import sess.implicits._
    val output = DBGenomics.transform("dummy", spark.emptyDataFrame, Map.empty[String, String])
    assert(output.count() == 1)
    assert(output.as[String].head() == "monkey")
  }
}

class DummyTransformer extends DataFrameTransformer {
  override def name: String = "dummy"

  override def transform(df: DataFrame, options: Map[String, String]): DataFrame = {
    df.sparkSession.createDataFrame(Seq(StringWrapper("monkey")))
  }
}

case class StringWrapper(s: String)
