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

package io.projectglow.sql.util

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.types.StructType

import io.projectglow.common.VCFRow

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
