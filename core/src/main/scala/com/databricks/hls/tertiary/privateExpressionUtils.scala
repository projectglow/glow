package org.apache.spark.sql.databricks.hls.tertiary // Visibility hack

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Expression,
  GetStructField,
  NamedExpression,
  Unevaluable
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

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
