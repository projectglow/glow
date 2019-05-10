package com.databricks.hls.tertiary

import org.apache.spark.sql.DataFrame
import org.apache.spark.unsafe.types.UTF8String

import com.databricks.hls.sql.HLSBaseTest

class VariantUtilExprsSuite extends HLSBaseTest {
  case class SimpleGenotype(calls: Seq[Int], phased: Boolean)
  case class SimpleGenotypeFields(genotype: SimpleGenotype)
  case class SimpleVariant(genotypes: Seq[SimpleGenotypeFields])

  private def makeGenotypesDf(calls: Seq[Seq[Int]]): DataFrame = {
    val genotypes = calls.map(c => SimpleGenotypeFields(SimpleGenotype(c, phased = true)))
    val variant = SimpleVariant(genotypes)
    spark.createDataFrame(Seq(variant))
  }

  private lazy val sess = spark

  logger.warn(s"Currently in ${System.getProperty("user.dir")}")

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
    TestCase("A", "*", VariantType.SpanningDeletion)) ++
    bases.flatMap(b1 => bases.map((b1, _))).collect { case (b1, b2) if b1 != b2 =>
    val sortedBases = Seq(b1, b2).sorted
    if (sortedBases == Seq("A", "G") || sortedBases == Seq("C", "T")) {
      TestCase(b1, b2, VariantType.Transition)
    } else {
      TestCase(b1, b2, VariantType.Transversion)
    }
  }

  gridTest("variant type")(testCases) { case TestCase(ref, alt, expected) =>
    val t = VariantUtilExprs.variantType(UTF8String.fromString(ref), UTF8String.fromString(alt))
    assert(t == expected)
  }
}
