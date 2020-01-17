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

package io.projectglow.vcf

import java.lang.{Double => JDouble}
import java.util.{List => JList}

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.{VariantContext => HtsjdkVariantContext}
import htsjdk.variant.vcf.{VCFEncoderUtils, VCFHeader}

import io.projectglow.common.{GlowLogging, VCFRow}

object VariantContextToVCFRowConverter {

  // Usually encodes the object normally, but does not pretty-print doubles
  def parseObjectAsString(obj: Object): String = {
    obj match {
      case d: JDouble => d.toString
      case dArray: Array[Double] =>
        val sArray = new Array[String](dArray.length)
        var i = 0
        while (i < dArray.length) {
          sArray(i) = dArray(i).toString
          i += 1
        }
        VCFEncoderUtils.formatVCFField(sArray)
      case dList: JList[Double] =>
        val sArray = new Array[String](dList.size)
        var i = 0
        while (i < dList.size) {
          sArray(i) = dList.get(i).toString
          i += 1
        }
        VCFEncoderUtils.formatVCFField(sArray)
      case _ => VCFEncoderUtils.formatVCFField(obj)
    }
  }
}

// HTSJDK VariantContext -> VCFRow
class VariantContextToVCFRowConverter(
    vcfHeader: VCFHeader,
    stringency: ValidationStringency = ValidationStringency.LENIENT,
    includeSampleIds: Boolean = true)
    extends GlowLogging
    with Serializable {

  private val converter = new VariantContextToInternalRowConverter(
    vcfHeader,
    VCFRow.schema,
    stringency,
    writeSampleIds = includeSampleIds
  )

  private val rowToVCFRowConverter = VCFRow.encoder.resolveAndBind()

  def convert(variantContext: HtsjdkVariantContext): VCFRow = {
    val internalRow = converter.convertRow(variantContext, isSplit = false)
    rowToVCFRowConverter.fromRow(internalRow)
  }
}
