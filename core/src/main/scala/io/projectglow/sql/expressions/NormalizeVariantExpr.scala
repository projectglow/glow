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

package io.projectglow.sql.expressions

import java.nio.file.Paths

import htsjdk.samtools.reference.{ReferenceSequenceFile, ReferenceSequenceFileFactory}
import io.projectglow.transformers.normalizevariants.VariantNormalizer

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes, SenaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object NormalizeVariantExpr {

  val state = new ThreadLocal[ReferenceSequenceFile]

  def doVariantNormalization(
      contigName: Any,
      start: Any,
      end: Any,
      refAllele: Any,
      altAlleles: Any,
      refGenomePathString: Any): InternalRow = {

    if (state.get() == null) {
      // Save fasta sequence file

      val refGenomeIndexedFasta = ReferenceSequenceFileFactory.getReferenceSequenceFile(
        Paths.get(refGenomePathString.asInstanceOf[UTF8String].toString))
      state.set(refGenomeIndexedFasta)
      TaskContext
        .get()
        .addTaskCompletionListener[Unit](
          _ => {
            state.get().close()
            state.remove()
          }
        )
    }

    VariantNormalizer.normalizeVariant(
      contigName.asInstanceOf[UTF8String].toString,
      start.asInstanceOf[Long],
      end.asInstanceOf[Long],
      refAllele.asInstanceOf[UTF8String].toString,
      altAlleles.asInstanceOf[ArrayData].toArray[UTF8String](StringType).map(_.toString),
      state.get()
    )
  }
}

case class NormalizeVariantExpr(
    contigName: Expression,
    start: Expression,
    end: Expression,
    refAllele: Expression,
    altAlleles: Expression,
    refGenomePathString: Expression)
    extends SenaryExpression
    with ImplicitCastInputTypes {

  override def prettyName: String = "normalize_variant"

  override def dataType: DataType = VariantNormalizer.normalizationResultStructType

  override def inputTypes: Seq[DataType] =
    Seq(StringType, LongType, LongType, StringType, ArrayType(StringType), StringType)

  override def children: Seq[Expression] =
    Seq(contigName, start, end, refAllele, altAlleles, refGenomePathString)

  override def checkInputDataTypes(): TypeCheckResult = {
    super.checkInputDataTypes()
    if (!refGenomePathString.foldable) {
      TypeCheckResult.TypeCheckFailure("Reference Genome Path must be a constant value")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override protected def nullSafeEval(
      contigName: Any,
      start: Any,
      end: Any,
      refAllele: Any,
      altAlleles: Any,
      refGenomePathString: Any): Any = {

    NormalizeVariantExpr.doVariantNormalization(
      contigName,
      start,
      end,
      refAllele,
      altAlleles,
      refGenomePathString)

  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      (contigName, start, end, refAllele, altAlleles, refGenomePathString) => {
        s"""
         |
         |${ev.value} = io.projectglow.sql.expressions.NormalizeVariantExpr.doVariantNormalization($contigName, $start, $end, $refAllele, $altAlleles, $refGenomePathString);
       """.stripMargin
      }
    )
  }
}
