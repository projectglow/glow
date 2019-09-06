package com.databricks.hls.transformers.pipe

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import com.databricks.hls.sql.HLSBaseTest

class TextPiperSuite extends HLSBaseTest {
  def pipeText(df: DataFrame): DataFrame = {
    val options =
      Map("inputFormatter" -> "text", "outputFormatter" -> "text", "cmd" -> """["cat", "-"]""")
    new PipeTransformer().transform(df, options)
  }

  test("text input and output") {
    val sess = spark
    import sess.implicits._

    val output = pipeText(Seq("hello", "world").toDF())
    assert(output.count() == 2)
    assert(output.schema == StructType(Seq(StructField("text", StringType))))
    assert(output.orderBy("text").as[String].collect.toSeq == Seq("hello", "world"))
  }

  test("text input requires one column") {
    val sess = spark
    import sess.implicits._

    val df = Seq(Seq("hello", "world"), Seq("foo", "bar")).toDF()
    assertThrows[IllegalArgumentException](pipeText(df))
  }

  test("text input requires string column") {
    val sess = spark
    import sess.implicits._

    val df = Seq(Seq(5), Seq(6)).toDF()
    assertThrows[IllegalArgumentException](pipeText(df))
  }

  test("does not break on null row") {
    val sess = spark
    import sess.implicits._

    val df = Seq("hello", null, "hello").toDF()
    val output = pipeText(df)
    assert(output.count() == 2)
    assert(output.filter("text = 'hello'").count == 2)
  }

  test("command fails") {
    val sess = spark
    import sess.implicits._

    val df = Seq("hello", "world").toDF()
    val options =
      Map(
        "inputFormatter" -> "text",
        "outputFormatter" -> "text",
        "cmd" -> """["bash", "-c", "exit 1"]""")

    val ex = intercept[SparkException] {
      new PipeTransformer().transform(df, options)
    }
    assert(ex.getMessage.contains("Subprocess exited with status 1"))

    // threads should still be cleaned up
    eventually {
      assert(
        !Thread
          .getAllStackTraces
          .asScala
          .keySet
          .exists(_.getName.startsWith(ProcessHelper.STDIN_WRITER_THREAD_PREFIX)))
      assert(
        !Thread
          .getAllStackTraces
          .asScala
          .keySet
          .exists(_.getName.startsWith(ProcessHelper.STDERR_READER_THREAD_PREFIX)))
    }
  }
}
