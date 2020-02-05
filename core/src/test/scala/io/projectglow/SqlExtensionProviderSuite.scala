package io.projectglow

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Literal, UnaryExpression}
import org.apache.spark.sql.types.{DataType, IntegerType}

import io.projectglow.sql.SqlExtensionProvider

class SqlExtensionProviderSuite extends GlowSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    SqlExtensionProvider.registerFunctions(
      spark.sessionState.conf,
      spark.sessionState.functionRegistry,
      "test-functions.yml")
  }

  private lazy val sess = spark
  test("one arg function") {
    import sess.implicits._
    assert(spark.range(1).selectExpr("one_arg_test(id)").as[Int].head() == 1)

    intercept[AnalysisException] {
      spark.range(1).selectExpr("one_arg_test()").collect()
    }

    intercept[AnalysisException] {
      spark.range(1).selectExpr("one_arg_test(id, id)").collect()
    }
  }

  test("two arg function") {
    import sess.implicits._
    assert(spark.range(1).selectExpr("two_arg_test(id, id)").as[Int].head() == 1)

    intercept[AnalysisException] {
      spark.range(1).selectExpr("two_arg_test(id)").collect()
    }

    intercept[AnalysisException] {
      spark.range(1).selectExpr("two_arg_test(id, id, id)").collect()
    }
  }

  test("var args function") {
    import sess.implicits._
    assert(spark.range(1).selectExpr("var_args_test(id, id)").as[Int].head() == 1)
    assert(spark.range(1).selectExpr("var_args_test(id, id, id, id)").as[Int].head() == 1)
    assert(spark.range(1).selectExpr("var_args_test(id)").as[Int].head() == 1)

    intercept[AnalysisException] {
      spark.range(1).selectExpr("var_args_test()").collect()
    }
  }

  test("can call optional arg function") {
    import sess.implicits._
    assert(spark.range(1).selectExpr("optional_arg_test(id)").as[Int].head() == 1)
    assert(spark.range(1).selectExpr("optional_arg_test(id, id)").as[Int].head() == 1)

    intercept[AnalysisException] {
      spark.range(1).selectExpr("optional_arg_test()").collect()
    }

    intercept[AnalysisException] {
      spark.range(1).selectExpr("optional_arg_test(id, id, id)").collect()
    }
  }
}

trait TestExpr extends Expression with CodegenFallback {
  override def dataType: DataType = IntegerType

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = 1
}

case class OneArgExpr(child: Expression) extends UnaryExpression with TestExpr
case class TwoArgExpr(left: Expression, right: Expression) extends BinaryExpression with TestExpr
case class VarArgsExpr(arg: Expression, varArgs: Seq[Expression]) extends TestExpr {
  override def children: Seq[Expression] = arg +: varArgs
}
case class OptionalArgExpr(required: Expression, optional: Expression) extends TestExpr {
  def this(required: Expression) = this(required, Literal(1))
  override def children: Seq[Expression] = Seq(required, optional)
}
