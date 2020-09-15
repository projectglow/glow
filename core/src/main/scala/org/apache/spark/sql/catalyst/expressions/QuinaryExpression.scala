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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen._

/**
 * An expression with four inputs + 5th optional input and one output.
 * The output is by default evaluated to null if any input is evaluated to null.
 */
abstract class QuinaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of QuinaryExpression.
   * If subclass of QuinaryExpression override nullable, probably should also override this.
   */
  override def eval(input: InternalRow): Any = {
    val exprs = children
    val v1 = exprs(0).eval(input)
    if (v1 != null) {
      val v2 = exprs(1).eval(input)
      if (v2 != null) {
        val v3 = exprs(2).eval(input)
        if (v3 != null) {
          val v4 = exprs(3).eval(input)
          if (v4 != null) {
            if (exprs.length > 4) {
              val v5 = exprs(4).eval(input)
              if (v5 != null) {
                return nullSafeEval(v1, v2, v3, v4, Some(v5))
              }
            } else {
              return nullSafeEval(v1, v2, v3, v4, None)
            }
          }
        }
      }
    }
    null
  }

  /**
   * Called by default [[eval]] implementation.  If subclass of QuinaryExpression keep the
   * default nullability, they can override this method to save null-check code.  If we need
   * full control of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(
      input1: Any,
      input2: Any,
      input3: Any,
      input4: Any,
      input5: Option[Any]): Any = {
    sys.error("QuinaryExpression must override either eval or nullSafeEval")
  }

  /**
   * Short hand for generating quinary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f accepts seven variable names and returns Java code to compute the output.
   */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, Option[String]) => String
  ): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2, eval3, eval4, eval5) => {
      s"${ev.value} = ${f(eval1, eval2, eval3, eval4, eval5)};"
    })
  }

  /**
   * Short hand for generating septenary evaluation code.
   * If either of the sub-expressions is null, the result of this computation
   * is assumed to be null.
   *
   * @param f function that accepts the 7 non-null evaluation result names of children
   *          and returns Java code to compute the output.
   */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String, String, String, Option[String]) => String
  ): ExprCode = {
    val firstGen = children(0).genCode(ctx)
    val secondGen = children(1).genCode(ctx)
    val thirdGen = children(2).genCode(ctx)
    val fourthGen = children(3).genCode(ctx)
    val fifthGen = if (children.length > 4) Some(children(4).genCode(ctx)) else None
    val resultCode =
      f(firstGen.value, secondGen.value, thirdGen.value, fourthGen.value, fifthGen.map(_.value))

    if (nullable) {
      val nullSafeEval =
        firstGen.code + ctx.nullSafeExec(children(0).nullable, firstGen.isNull) {
          secondGen.code + ctx.nullSafeExec(children(1).nullable, secondGen.isNull) {
            thirdGen.code + ctx.nullSafeExec(children(2).nullable, thirdGen.isNull) {
              fourthGen.code + ctx.nullSafeExec(children(3).nullable, fourthGen.isNull) {
                val nullSafeResultCode =
                  s"""
                      ${ev.isNull} = false; // resultCode could change nullability.
                      $resultCode
                      """
                fifthGen.map { gen =>
                  gen.code + ctx.nullSafeExec(children(4).nullable, gen.isNull) {
                    nullSafeResultCode
                  }
                }.getOrElse(nullSafeResultCode)
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
        ${fifthGen.map(_.code).getOrElse("")}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        $resultCode""",
        isNull = FalseLiteral
      )
    }
  }
}
