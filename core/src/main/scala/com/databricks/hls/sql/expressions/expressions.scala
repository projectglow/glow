package com.databricks.hls.sql.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression

abstract class QuaternaryExpression extends Expression {

  override def foldable: Boolean = children.forall(_.foldable)

  override def nullable: Boolean = children.exists(_.nullable)

  /**
   * Default behavior of evaluation according to the default nullability of TernaryExpression.
   * If subclass of TernaryExpression override nullable, probably should also override this.
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
   * Called by default [[eval]] implementation.  If subclass of TernaryExpression keep the default
   * nullability, they can override this method to save null-check code.  If we need full control
   * of evaluation process, we should override [[eval]].
   */
  protected def nullSafeEval(input1: Any, input2: Any, input3: Any, input4: Any): Any =
    sys.error(s"QuaternaryExpressions must override either eval or nullSafeEval")
}
