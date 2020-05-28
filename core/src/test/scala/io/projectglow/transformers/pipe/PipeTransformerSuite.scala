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

package io.projectglow.transformers.pipe

import java.io.{InputStream, OutputStream}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.Glow
import io.projectglow.sql.GlowBaseTest

class PipeTransformerSuite extends GlowBaseTest {
  test("cleanup") {
    sparkContext.getPersistentRDDs.values.foreach(_.unpersist(true))
    val sess = spark
    import sess.implicits._
    val df = Seq("dolphin").toDF.repartition(1)
    df.rdd.cache()
    val options =
      Map("inputFormatter" -> "dummy_in", "outputFormatter" -> "dummy_out", "cmd" -> """["cat"]""")
    new PipeTransformer().transform(df, options)
    assert(sparkContext.getPersistentRDDs.size == 2)
    Glow.transform("pipe_cleanup", df, Map.empty[String, String])
    eventually {
      assert(sparkContext.getPersistentRDDs.size == 1) // Should cleanup the RDD cached by piping
    }

    df.rdd.unpersist()
  }

  test("read input and output formatters from service loader") {
    val sess = spark
    import sess.implicits._

    val df = Seq("dolphin").toDF.repartition(1)
    val options =
      Map("inputFormatter" -> "dummy_in", "outputFormatter" -> "dummy_out", "cmd" -> """["cat"]""")
    val output = new PipeTransformer().transform(df, options)
    assert(output.count() == 1)
    assert(output.schema.length == 1)
    assert(output.schema.exists(f => f.name == "animal" && f.dataType == StringType))
    assert(output.where("animal = 'monkey'").count() == 1)

    Glow.transform("pipe_cleanup", df, Map.empty[String, String])
  }

  test("missing input formatter") {
    val df = spark.emptyDataFrame
    val options =
      Map("outputFormatter" -> "dummy_out", "cmd" -> """["cat"]""")
    val e = intercept[IllegalArgumentException] {
      new PipeTransformer().transform(df, options)
    }
    assert(e.getMessage.contains("Missing pipe input formatter"))
  }

  test("missing output formatter") {
    val df = spark.emptyDataFrame
    val options =
      Map("inputFormatter" -> "dummy_in", "cmd" -> """["cat"]""")
    val e = intercept[IllegalArgumentException] {
      new PipeTransformer().transform(df, options)
    }
    assert(e.getMessage.contains("Missing pipe output formatter"))
  }

  test("could not find input formatter") {
    val df = spark.emptyDataFrame
    val options =
      Map("inputFormatter" -> "fake_in", "outputFormatter" -> "dummy_out", "cmd" -> """["cat"]""")
    val e = intercept[IllegalArgumentException] {
      new PipeTransformer().transform(df, options)
    }
    assert(e.getMessage.contains("Could not find an input formatter for fake_in"))
  }

  test("could not find output formatter") {
    val df = spark.emptyDataFrame
    val options =
      Map("inputFormatter" -> "dummy_in", "outputFormatter" -> "fake_out", "cmd" -> """["cat"]""")
    val e = intercept[IllegalArgumentException] {
      new PipeTransformer().transform(df, options)
    }
    assert(e.getMessage.contains("Could not find an output formatter for fake_out"))
  }

  test("pass command as a Seq") {
    val sess = spark
    import sess.implicits._
    val inputDf = spark.createDataFrame(Seq(Tuple1("monkey")))
    val options = Map(
      "inputFormatter" -> "text",
      "outputFormatter" -> "text",
      "cmd" -> Seq("sed", "-e", "s/$/!/"))
    val output = Glow.transform("pipe", inputDf, options).as[String].head
    assert(output == "monkey!")
    Glow.transform("pipe_cleanup", inputDf, Map.empty[String, String])
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
  override def makeIterator(stream: InputStream): Iterator[Any] = {
    val schema = StructType(Seq(StructField("animal", StringType)))
    val internalRow = new GenericInternalRow(
      Array(UTF8String.fromString("monkey")).asInstanceOf[Array[Any]])
    Iterator(schema, internalRow)
  }
}
