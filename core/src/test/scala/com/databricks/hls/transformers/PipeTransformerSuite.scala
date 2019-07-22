package com.databricks.hls.transformers

import java.io.{InputStream, OutputStream}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.hls.sql.HLSBaseTest

class PipeTransformerSuite extends HLSBaseTest {
  test("read input and output formatters from service loader") {
    val df = spark.emptyDataFrame.repartition(1)
    val options =
      Map("inputFormatter" -> "dummy_in", "outputFormatter" -> "dummy_out", "cmd" -> """["cat"]""")
    val output = new PipeTransformer().transform(df, options)
    assert(output.count() == 1)
    assert(output.schema.length == 1)
    assert(output.schema.exists(f => f.name == "animal" && f.dataType == StringType))
    assert(output.where("animal = 'monkey'").count() == 1)
  }

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
}

class DummyInputFormatterFactory() extends InputFormatterFactory {
  def name: String = "dummy_in"

  override def makeInputFormatter(df: DataFrame, options: Map[String, String]): InputFormatter = {
    new DummyInputFormatter()
  }
}

class DummyInputFormatter() extends InputFormatter {
  override def close(): Unit = ()

  override def writeDummyDataset(): Unit = ()

  override def write(record: InternalRow): Unit = ()

  override def init(stream: OutputStream): Unit = ()
}

class DummyOutputFormatterFactory() extends OutputFormatterFactory {
  override def name: String = "dummy_out"

  override def makeOutputFormatter(options: Map[String, String]): OutputFormatter = {
    new DummyOutputFormatter()
  }
}

class DummyOutputFormatter() extends OutputFormatter {
  override def makeIterator(schema: StructType, stream: InputStream): Iterator[InternalRow] = {
    Seq(new GenericInternalRow(Array[Any](UTF8String.fromString("monkey")))).iterator
  }

  override def outputSchema(stream: InputStream): StructType = {
    StructType(Seq(StructField("animal", StringType)))
  }
}
