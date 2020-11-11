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

package io.projectglow.tertiary

import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, SparseVector, Vector}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import io.projectglow.sql.GlowBaseTest
import io.projectglow.sql.expressions.{VariantType, VariantUtilExprs}
import io.projectglow.functions._

class VariantUtilExprsSuite extends GlowBaseTest {
  case class SimpleGenotypeFields(calls: Seq[Int])
  case class SimpleVariant(genotypes: Seq[SimpleGenotypeFields])

  private def makeGenotypesDf(calls: Seq[Seq[Int]]): DataFrame = {
    val genotypes = calls.map(c => SimpleGenotypeFields(c))
    val variant = SimpleVariant(genotypes)
    spark.createDataFrame(Seq(variant))
  }

  private lazy val sess = spark

  test("simple cases") {
    import sess.implicits._
    val states = makeGenotypesDf(Seq(Seq(0, 0), Seq(1, 0), Seq(2, 2)))
      .selectExpr("genotype_states(genotypes)")
      .as[Seq[Int]]
      .head()
    assert(states == Seq(0, 1, 4))
  }

  test("-1 if any -1 appears in call array") {
    import sess.implicits._
    val states = makeGenotypesDf(Seq(Seq(0, -1)))
      .selectExpr("genotype_states(genotypes)")
      .as[Seq[Int]]
      .head
    assert(states == Seq(-1))
  }

  test("-1 if call array is empty") {
    import sess.implicits._
    val states = makeGenotypesDf(Seq(Seq(), Seq(1, 1)))
      .selectExpr("genotype_states(genotypes)")
      .as[Seq[Int]]
      .head
    assert(states == Seq(-1, 2))
  }

  case class TestCase(ref: String, alt: String, vt: VariantType)
  val bases = Seq("A", "C", "G", "T")
  val testCases = Seq(
      TestCase("", "", VariantType.Unknown),
      TestCase("ACG", "ATG", VariantType.Transition),
      TestCase("AG", "ATG", VariantType.Insertion),
      TestCase("AG", "ATCTCAG", VariantType.Insertion),
      TestCase("ATG", "A", VariantType.Deletion),
      TestCase("ACTGGGG", "AG", VariantType.Deletion),
      TestCase("A", "*", VariantType.SpanningDeletion)
    ) ++
    bases.flatMap(b1 => bases.map((b1, _))).collect {
      case (b1, b2) if b1 != b2 =>
        val sortedBases = Seq(b1, b2).sorted
        if (sortedBases == Seq("A", "G") || sortedBases == Seq("C", "T")) {
          TestCase(b1, b2, VariantType.Transition)
        } else {
          TestCase(b1, b2, VariantType.Transversion)
        }
    }

  gridTest("variant type")(testCases) {
    case TestCase(ref, alt, expected) =>
      val t = VariantUtilExprs.variantType(UTF8String.fromString(ref), UTF8String.fromString(alt))
      assert(t == expected)
  }

  test("add struct field has correct schema") {
    import io.projectglow.functions._
    val df = spark.createDataFrame(Seq(Outer(Inner(1, "monkey"))))
    val added = df.select(
      add_struct_fields(col("inner"), lit("number"), lit(1), lit("string"), lit("blah"))
        .as("struct"))
    val inner = added.schema.find(_.name == "struct").get.dataType.asInstanceOf[StructType]
    val fields = Seq(
      ("one", IntegerType),
      ("two", StringType),
      ("number", IntegerType),
      ("string", StringType)
    )
    assert(inner.length == fields.size)
    fields.foreach {
      case (name, typ) =>
        assert(inner.exists(f => f.name == name && f.dataType == typ))
    }
  }

  test("add struct field has correct values") {
    import sess.implicits._
    val value = spark
      .createDataFrame(Seq(Outer(Inner(1, "monkey"))))
      .selectExpr(
        "expand_struct(add_struct_fields(inner, 'three', " +
        "cast(3.14159 as double), 'four', true))"
      )
      .as[BigInner]
      .head
    assert(value == BigInner(1, "monkey", 3.14159, true))
  }

  private val hcTestCases = Seq(
    HCTestCase(Seq(0.0, 0.0, 1.0), Some(1), Seq(1, 1), Some(false), None, "unphased"),
    HCTestCase(
      Seq(0.1, 0.1, 0.8),
      Some(1),
      Seq(-1, -1),
      Some(false),
      None,
      "unphased, below threshold"
    ),
    HCTestCase(
      Seq(0.1, 0.9, 0.8, 0.2),
      Some(1),
      Seq(1, -1),
      Some(true),
      None,
      "phased, 1 below threshold"
    ),
    HCTestCase(
      Seq(0.1, 0.9, 0.8, 0.2),
      Some(1),
      Seq(1, 0),
      Some(true),
      Some(0.8),
      "phased, lower threshold"
    ),
    HCTestCase(Seq(0, 1, 0, 0, 0, 1), Some(2), Seq(1, 2), Some(true), Some(0.8), "phased, 2 alts"),
    HCTestCase(
      Seq(0.1, 0.9, 0.0, 0.1, 0.1, 0.8),
      Some(2),
      Seq(1, 2),
      Some(true),
      Some(0.8),
      "phased 2 alts (2)"
    ),
    HCTestCase(
      Seq(0.1, 0.1, 0.8),
      Some(1),
      Seq(1, 1),
      Some(false),
      Some(0.8),
      "unphased, lower threshold"
    ),
    HCTestCase(Seq(0, 0, 0, 0, 1, 0), Some(2), Seq(1, 2), Some(false), None, "unphased, 2 alts"),
    HCTestCase(
      Seq(0, 0, 0, 1, 0, 0),
      Some(2),
      Seq(0, 2),
      Some(false),
      None,
      "unphased, 2 alts (2)"
    ),
    HCTestCase(
      Seq(0, 0, 0, 0, 0, 0, 0, 1, 0, 0),
      Some(3),
      Seq(1, 3),
      Some(false),
      None,
      "unphased, 3 alts"
    ),
    HCTestCase(null, Some(1), null, Some(false), None, "null probabilities"),
    HCTestCase(Seq(1, 2), None, null, Some(false), None, "null num alts"),
    HCTestCase(Seq(1, 2), Some(1), null, None, None, "null phasing")
  )

  gridTest("hard calls")(hcTestCases) { testCase =>
    import sess.implicits._
    val thresholdStr = testCase.threshold.map(d => s", $d").getOrElse("")
    val input = spark.createDataFrame(Seq(testCase))
    val outputDF = input
      .withColumn("calls", expr(s"hard_calls(probabilities, numAlts, phased $thresholdStr)"))
    val output = outputDF
      .as[HCTestCase]
      .head
    assert(output.calls == testCase.calls)
  }

  test("hard calls casts input") {
    import sess.implicits._
    val res = spark
      .range(1)
      .selectExpr("hard_calls(array(0, 0, 1), cast(1 as bigint), false, 0.8)")
      .as[Seq[Int]]
      .head
    assert(res == Seq(1, 1))
  }

  test("hard calls threshold must be constant") {
    val e = intercept[AnalysisException] {
      spark
        .range(1)
        .selectExpr("hard_calls(array(0, 0, 1), cast(1 as bigint), false, rand())")
        .head
    }
    assert(e.getMessage.contains("Threshold must be a constant value"))
  }

  private val arrays: Seq[Seq[Double]] = Seq(
    Seq.empty,
    Seq(1, 2, 3),
    Seq(Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN),
    Seq(-1, -1.23, 3.14159)
  )

  gridTest("to/from sparse vector")(arrays) { array =>
    import sess.implicits._
    val vectorDf = sess
      .createDataFrame(Seq(DoubleArrayWrapper(array)))
      .selectExpr("array_to_sparse_vector(features) as features")

    val vector = vectorDf
      .as[VectorWrapper]
      .head
      .features
    assert(vector.isInstanceOf[SparseVector])
    assertSeqsMatch(array, vector.toArray)

    val convertedArray = vectorDf
      .selectExpr("vector_to_array(features) as features")
      .as[DoubleArrayWrapper]
      .head
      .features
    assertSeqsMatch(array, convertedArray)
  }

  gridTest("to/from dense vector")(arrays) { array =>
    import sess.implicits._
    val vectorDf = sess
      .createDataFrame(Seq(DoubleArrayWrapper(array)))
      .selectExpr("array_to_dense_vector(features) as features")

    val vector = vectorDf
      .as[VectorWrapper]
      .head
      .features
    assert(vector.isInstanceOf[DenseVector])
    assertSeqsMatch(array, vector.toArray)

    val convertedArray = vectorDf
      .selectExpr("vector_to_array(features) as features")
      .as[DoubleArrayWrapper]
      .head
      .features
    assertSeqsMatch(array, convertedArray)
  }

  test("cast input when converting to vector") {
    import sess.implicits._
    val seq = spark
      .range(1)
      .selectExpr("array_to_sparse_vector(cast(array(1, 2, 3) as array<int>)) as features")
      .selectExpr("vector_to_array(features)")
      .as[Seq[Double]]
      .head
    assert(seq == Seq(1d, 2d, 3d))
  }

  private def assertSeqsMatch(s1: Seq[Double], s2: Seq[Double]): Unit = {
    s1.zip(s2).foreach {
      case (d1, d2) =>
        assert(d1 == d2 || (d1.isNaN && d2.isNaN))
    }
  }

  private val baseMatrix = new DenseMatrix(4, 3, (1 to 12).map(_.toDouble).toArray)
  private val matrices = Seq(
    ("dense col major", baseMatrix.toDenseColMajor),
    ("dense row major", baseMatrix.toDenseRowMajor),
    ("sparse col major", baseMatrix.toSparseColMajor),
    ("sparse row major", baseMatrix.toSparseRowMajor)
  )
  gridTest("explode matrix")(matrices) {
    case (_, matrix) =>
      import sess.implicits._
      val exploded = spark
        .createDataFrame(Seq(Tuple1(matrix)))
        .selectExpr("explode_matrix(_1)")
        .as[Seq[Double]]
        .collect()
      val expected = Seq(
        Seq(1, 5, 9),
        Seq(2, 6, 10),
        Seq(3, 7, 11),
        Seq(4, 8, 12)
      )
      assert(exploded.toSeq == expected)
  }

  test("explode matrix (null)") {
    assert(spark.sql("select explode_matrix(null)").count() == 0)
  }

  test("subset struct") {
    import io.projectglow.functions._
    val df = spark.createDataFrame(Seq(BigOuter(BigInner(1, "monkey", 2.5, false))))
    val subsetted =
      df.select(subset_struct(col("bigInner"), "one", "three").as("struct"))
    assert(
      subsetted.schema.find(_.name == "struct").get.dataType.asInstanceOf[StructType] == StructType(
        Seq(StructField("one", IntegerType), StructField("three", DoubleType))))
    assert(subsetted.select("struct").collect.head == Row(Row(1, 2.5)))
  }

  test("expand struct only works on structs") {
    val df = spark.createDataFrame(Seq(Outer(Inner(1, "monkey"))))
    val e = intercept[AnalysisException] { df.selectExpr("expand_struct(inner.two)").show() }
    assert(e.getMessage.contains("Only structs can be expanded"))
  }

  test("expand struct scala") {
    import sess.implicits._
    val df = spark.createDataFrame(Seq(Outer(Inner(1, "two"))))
    assert(df.select(expand_struct(col("inner"))).as[Inner].head == Inner(1, "two"))
  }
}

case class HCTestCase(
    probabilities: Seq[Double],
    numAlts: Option[Int],
    calls: Seq[Int],
    phased: Option[Boolean],
    threshold: Option[Double],
    name: String) {
  override def toString: String = name
}

case class Inner(one: Int, two: String)
case class BigInner(one: Int, two: String, three: Double, four: Boolean)
case class Outer(inner: Inner)
case class BigOuter(bigInner: BigInner)
case class DoubleArrayWrapper(features: Seq[Double])
case class VectorWrapper(features: Vector)
