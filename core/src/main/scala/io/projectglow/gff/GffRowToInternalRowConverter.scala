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

package io.projectglow.gff

import io.projectglow.common.GlowLogging
import io.projectglow.gff.GffFileFormat._
import io.projectglow.common.FeatureSchemas._
import io.projectglow.sql.util.RowConverter

import org.apache.spark.sql.SQLUtils.{dataTypesEqualExceptNullability, structFieldsEqualExceptNullability}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class GffRowToInternalRowConverter(
    parserSchema: StructType,
    requiredSchema: StructType
) extends GlowLogging {

  private var delimiter: Option[Char] = None

  private val converter = {
    val fns = requiredSchema.map { field =>
      val fn: RowConverter.Updater[(InternalRow, Map[String, String])] = field match {
        case f
            if Seq(
              seqIdField,
              sourceField,
              typeField,
              attributesField // if attributesField is in a user specified schema it will be passed through as well.
            ).exists(structFieldsEqualExceptNullability(f, _)) =>
          (rowAndMap, gffRow, idx) => updateStringFields(rowAndMap._1, gffRow, idx)
        case f if structFieldsEqualExceptNullability(f, startField) =>
          (rowAndMap, gffRow, idx) => updateStart(rowAndMap._1, gffRow, idx)
        case f if structFieldsEqualExceptNullability(f, endField) =>
          (rowAndMap, gffRow, idx) => updateEnd(rowAndMap._1, gffRow, idx)
        case f if structFieldsEqualExceptNullability(f, scoreField) =>
          (rowAndMap, gffRow, idx) => updateScore(rowAndMap._1, gffRow, idx)
        case f if structFieldsEqualExceptNullability(f, strandField) =>
          (rowAndMap, gffRow, idx) => updateStrand(rowAndMap._1, gffRow, idx)
        case f if structFieldsEqualExceptNullability(f, phaseField) =>
          (rowAndMap, gffRow, idx) => updatePhase(rowAndMap._1, gffRow, idx)
        case f =>
          (rowAndMap, gffRow, idx) => {
            val value = rowAndMap._2.get(deUnderscore(f.name.toLowerCase))
            value match {
              case None => ()
              case Some(v) =>
                if (dataTypesEqualExceptNullability(f.dataType, StringType)) {
                  updateStringAttribute(v, gffRow, idx)
                } else if (dataTypesEqualExceptNullability(f.dataType, ArrayType(StringType))) {
                  updateStringArrayAttribute(v, gffRow, idx)
                } else if (dataTypesEqualExceptNullability(f.dataType, BooleanType)) {
                  updateBooleanAttribute(v, gffRow, idx)
                } else {
                  ()
                }
            }
          }
      }
      fn
    }
    new RowConverter[(InternalRow, Map[String, String])](requiredSchema, fns.toArray)
  }

  def convertRow(csvRow: InternalRow): InternalRow = {
    val attrStr = csvRow.getString(parserSchema.fieldIndex(attributesField.name))
    val tagValueMap = parseAttributes(attrStr)
    converter((csvRow, tagValueMap))
  }

  def parseAttributes(attrStr: String): Map[String, String] = {

    val attributes = attrStr.split(ATTRIBUTES_DELIMITER)

    if (delimiter.isEmpty) {
      delimiter = if (attributes(0).contains(GFF3_TAG_VALUE_DELIMITER)) {
        Some(GFF3_TAG_VALUE_DELIMITER)
      } else {
        Some(GTF_TAG_VALUE_DELIMITER)
      }
    }

    var i = 0
    var attrMap = Map[String, String]()

    while (i < attributes.length) {
      val tag = attributes(i).takeWhile(_ != delimiter.get).toLowerCase
      val value = attributes(i).drop(tag.length + 1).trim
      val trimmedTag = deUnderscore(tag.trim.toLowerCase)
      attrMap += trimmedTag -> value
      i += 1
    }
    attrMap
  }

  // TODO: Add validation stringency for parsing

  private def updateStringFields(csvRow: InternalRow, gffRow: InternalRow, idx: Int): Unit = {
    gffRow.update(idx, csvRow.getUTF8String(idx))
  }

  private def updateStart(csvRow: InternalRow, gffRow: InternalRow, idx: Int): Unit = {
    gffRow.setLong(idx, csvRow.getString(idx).toLong - 1)
  }

  private def updateEnd(csvRow: InternalRow, gffRow: InternalRow, idx: Int): Unit = {
    gffRow.setLong(idx, csvRow.getString(idx).toLong)
  }

  private def updateScore(csvRow: InternalRow, gffRow: InternalRow, idx: Int): Unit = {
    val csvScore = csvRow.getString(idx)
    if (csvScore != NULL_IDENTIFIER) {
      gffRow.setDouble(idx, csvScore.toDouble)
    }
  }

  private def updateStrand(csvRow: InternalRow, gffRow: InternalRow, idx: Int): Unit = {
    val csvStrand = csvRow.getUTF8String(idx)
    if (csvStrand != UTF8String.fromString(NULL_IDENTIFIER)) {
      gffRow.update(idx, csvStrand)
    }
  }

  private def updatePhase(csvRow: InternalRow, gffRow: InternalRow, idx: Int): Unit = {
    val csvPhase = csvRow.getString(idx)
    if (csvPhase != NULL_IDENTIFIER) {
      gffRow.update(idx, csvPhase.toInt)
    }
  }

  private def updateStringAttribute(value: String, gffRow: InternalRow, idx: Int): Unit = {
    gffRow.update(idx, UTF8String.fromString(value))
  }

  private def updateStringArrayAttribute(value: String, gffRow: InternalRow, idx: Int): Unit = {
    val splits = value.split(ARRAY_DELIMITER)
    val arr = new Array[UTF8String](splits.length)
    var i = 0
    while (i < splits.length) {
      arr(i) = UTF8String.fromString(splits(i))
      i += 1
    }
    gffRow.update(idx, new GenericArrayData(arr))
  }

  private def updateBooleanAttribute(value: String, gffRow: InternalRow, idx: Int): Unit = {
    gffRow.setBoolean(idx, value.toBoolean)
  }
}
