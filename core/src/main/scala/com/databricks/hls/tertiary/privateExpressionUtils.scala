package org.apache.spark.sql.databricks.hls.tertiary // Visibility hack

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.{Star, TypeCheckResult}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Expression,
  GetStructField,
  NamedExpression,
  Unevaluable
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Expands all the fields of a potentially unnamed struct.
 */
case class ExpandStruct(struct: Expression) extends Star with Unevaluable {
  override def expand(input: LogicalPlan, resolver: analysis.Resolver): Seq[NamedExpression] = {
    if (!struct.dataType.isInstanceOf[StructType]) {
      throw new AnalysisException("Only structs can be expanded.")
    }

    struct.dataType.asInstanceOf[StructType].zipWithIndex.map {
      case (f, i) =>
        Alias(GetStructField(struct, i), f.name)()
    }
  }
}

/**
 * Expression that adds fields to an existing struct.
 *
 * At optimization time, this expression is rewritten as the creation of new struct with all the
 * fields of the existing struct as well as the new fields. See [[HLSReplaceExpressionsSuite]]
 * for more details.
 */
case class AddStructFields(struct: Expression, newFields: Seq[Expression])
    extends Expression
    with Unevaluable {

  override def nullable: Boolean = true
  override def children: Seq[Expression] = struct +: newFields
  override def dataType: DataType = {
    var base = struct.dataType.asInstanceOf[StructType]
    newFields.grouped(2).foreach {
      case Seq(name, value) =>
        val nameStr = name.eval().toString
        base = base.add(nameStr, value.dataType, value.nullable)
    }
    base
  }
}
