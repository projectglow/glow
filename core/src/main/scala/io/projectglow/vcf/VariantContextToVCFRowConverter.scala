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

import java.lang.{Boolean => JBoolean, Iterable => JIterable}

import scala.collection.mutable

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.{VariantContext => HtsjdkVariantContext}
import htsjdk.variant.vcf.{VCFConstants, VCFHeader}

import io.projectglow.common.{GlowLogging, VCFRow}

// HTSJDK VariantContext -> VCFRow
// Based on the HTSJDK classes VCFEncoder and CommonInfo
object VariantContextToVCFRowConverter {

  def parseObjectAsString(obj: Object): String = {
    obj match {
      case null => ""
      case VCFConstants.MISSING_VALUE_v4 => ""
      case _: JBoolean => ""
      case intArray: Array[Int] => intArray.mkString(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR)
      case doubleArray: Array[Double] =>
        doubleArray
          .mkString(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR)
      case objArray: Array[Object] =>
        val length = objArray.length
        val strSeq = new mutable.ArraySeq[String](length)
        var arrayIdx = 0
        while (arrayIdx < length) {
          strSeq.update(arrayIdx, parseObjectAsString(objArray(arrayIdx)))
          arrayIdx += 1
        }
        strSeq.mkString(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR)
      case objIter if objIter.isInstanceOf[JIterable[_]] =>
        val iterator = objIter.asInstanceOf[JIterable[Object]].iterator
        val listBuffer = new mutable.ListBuffer[String]()
        while (iterator.hasNext) {
          listBuffer.append(parseObjectAsString(iterator.next))
        }
        listBuffer.mkString(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR)
      case _ => obj.toString
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
