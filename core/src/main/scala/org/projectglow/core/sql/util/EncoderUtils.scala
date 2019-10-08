package org.projectglow.core.sql.util

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.types.StructType
import org.projectglow.core.common.VCFRow

object EncoderUtils {

  lazy val vcfRowEncoder: ExpressionEncoder[VCFRow] =
    Encoders.product[VCFRow].asInstanceOf[ExpressionEncoder[VCFRow]]

  /**
   * Filters out the parts of an encoder that are not contained in the provided schema.
   *
   * The `requiredSchema` must be a subset of encoder schema.
   * @return A new encoder that only outputs fields contained in the `requiredSchema`
   */
  def subsetEncoder[T](
      encoder: ExpressionEncoder[T],
      requiredSchema: StructType): ExpressionEncoder[T] = {
    val newSerializer = encoder.serializer.filter {
      case e: NamedExpression => requiredSchema.fieldNames.contains(e.name)
      case _ => true
    }
    encoder.copy(serializer = newSerializer, schema = requiredSchema)
  }
}
