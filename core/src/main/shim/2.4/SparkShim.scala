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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.trees.TreeNode

// Spark 2.4 APIs that are not inter-version compatible
object SparkShim extends SparkShimBase {
  override type CSVOptions = org.apache.spark.sql.execution.datasources.csv.CSVOptions
  override type UnivocityParser = org.apache.spark.sql.execution.datasources.csv.UnivocityParser

  override def wrapUnivocityParse(parser: UnivocityParser)(input: String): Option[InternalRow] = {
    Some(parser.parse(input))
  }

  override def createExpressionInfo(
      className: String,
      db: String,
      name: String,
      usage: String,
      arguments: String,
      examples: String,
      note: String,
      since: String): ExpressionInfo = {
    new ExpressionInfo(
      className,
      db,
      name,
      usage,
      arguments,
      examples,
      note,
      since
    )
  }

  /**
   * An expression with four inputs and one output. The output is by default evaluated to null
   * if any input is evaluated to null.
   */
  abstract class QuaternaryExpression extends Expression {

    override def foldable: Boolean = children.forall(_.foldable)

    override def nullable: Boolean = children.exists(_.nullable)

    /**
     * Default behavior of evaluation according to the default nullability of QuaternaryExpression.
     * If subclass of QuaternaryExpression override nullable, probably should also override this.
     */
    override def eval(input: InternalRow): Any = {
      val exprs = children
      val value1 = exprs(0).eval(input)
      if (value1 != null) {
        val value2 = exprs(1).eval(input)
        if (value2 != null) {
          val value3 = exprs(2).eval(input)
          if (value3 != null) {
            val value4 = exprs(3).eval(input)
            if (value4 != null) {
              return nullSafeEval(value1, value2, value3, value4)
            }
          }
        }
      }
      null
    }

    /**
     * Called by default [[eval]] implementation.  If subclass of QuaternaryExpression keep the
     *  default nullability, they can override this method to save null-check code.  If we need
     *  full control of evaluation process, we should override [[eval]].
     */
    protected def nullSafeEval(input1: Any, input2: Any, input3: Any, input4: Any): Any =
      sys.error(s"QuaternaryExpressions must override either eval or nullSafeEval")

    /**
     * Short hand for generating quaternary evaluation code.
     * If either of the sub-expressions is null, the result of this computation
     * is assumed to be null.
     *
     * @param f accepts four variable names and returns Java code to compute the output.
     */
    protected def defineCodeGen(
        ctx: CodegenContext,
        ev: ExprCode,
        f: (String, String, String, String) => String): ExprCode = {
      nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3, eval4) => {
        s"${ev.value} = ${f(eval1, eval2, eval3, eval4)};"
      })
    }

    /**
     * Short hand for generating quaternary evaluation code.
     * If either of the sub-expressions is null, the result of this computation
     * is assumed to be null.
     *
     * @param f function that accepts the 4 non-null evaluation result names of children
     *          and returns Java code to compute the output.
     */
    protected def nullSafeCodeGen(
        ctx: CodegenContext,
        ev: ExprCode,
        f: (String, String, String, String) => String): ExprCode = {
      val firstGen = children(0).genCode(ctx)
      val secondGen = children(1).genCode(ctx)
      val thirdGen = children(2).genCode(ctx)
      val fourthGen = children(3).genCode(ctx)
      val resultCode = f(firstGen.value, secondGen.value, thirdGen.value, fourthGen.value)

      if (nullable) {
        val nullSafeEval =
          firstGen.code + ctx.nullSafeExec(children(0).nullable, firstGen.isNull) {
            secondGen.code + ctx.nullSafeExec(children(1).nullable, secondGen.isNull) {
              thirdGen.code + ctx.nullSafeExec(children(2).nullable, thirdGen.isNull) {
                fourthGen.code + ctx.nullSafeExec(children(3).nullable, fourthGen.isNull) {
                  s"""
                  ${ev.isNull} = false; // resultCode could change nullability.
                  $resultCode
                """
                }
              }
            }
          }

        ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $nullSafeEval""")
      } else {
        ev.copy(
          code = code"""
        ${firstGen.code}
        ${secondGen.code}
        ${thirdGen.code}
        ${fourthGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""",
          isNull = FalseLiteral
        )
      }
    }
  }

  def newUnresolvedException[TreeType <: TreeNode[_]](
      tree: TreeType,
      function: String): Exception = {
    new UnresolvedException(tree, function)
  }

  abstract class TernaryExpression
      extends org.apache.spark.sql.catalyst.expressions.TernaryExpression {

    def first: Expression
    def second: Expression
    def third: Expression
    override def children: Seq[Expression] = Seq(first, second, third)
  }

  def getDateFormat(options: CSVOptions): String =
    options.parameters.getOrElse("dateFormat", "yyyy-MM-dd")

  def getTimestampFormat(options: CSVOptions): String =
    options.parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
}
