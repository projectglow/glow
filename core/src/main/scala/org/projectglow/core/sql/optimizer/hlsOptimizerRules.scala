package org.projectglow.core.sql.optimizer

import org.apache.spark.sql.catalyst.expressions.{CreateNamedStruct, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StructType
import org.projectglow.core.sql.expressions.AddStructFields

/**
 * Simple optimization rule that handles expression rewrites
 */
object HLSReplaceExpressionsRule extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
    case AddStructFields(struct, newFields) =>
      val baseType = struct.dataType.asInstanceOf[StructType]
      val baseFields = baseType.indices.flatMap { idx =>
        Seq(Literal(baseType(idx).name), GetStructField(struct, idx))
      }
      CreateNamedStruct(baseFields ++ newFields)
  }
}
