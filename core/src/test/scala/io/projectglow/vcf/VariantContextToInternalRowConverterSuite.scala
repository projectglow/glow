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

import scala.collection.JavaConverters._

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.{Allele, VariantContextBuilder}
import htsjdk.variant.vcf.{VCFHeader, VCFHeaderLine, VCFHeaderLineType, VCFInfoHeaderLine}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

import io.projectglow.sql.GlowBaseTest

class VariantContextToInternalRowConverterSuite extends GlowBaseTest {

  gridTest("Array fields converted to strings")(Seq(true, false)) { arrToStr =>
    val intArrayHeaderLine =
      new VCFInfoHeaderLine("IntArray", 2, VCFHeaderLineType.Integer, "Integer array")
    val strArrayHeaderLine =
      new VCFInfoHeaderLine("StringArray", 3, VCFHeaderLineType.String, "String array")
    val floatArrayHeaderLine =
      new VCFInfoHeaderLine("FloatArray", 4, VCFHeaderLineType.Float, "Float array")
    val headerLines = Seq(intArrayHeaderLine, strArrayHeaderLine, floatArrayHeaderLine)

    val vcfHeader = new VCFHeader(headerLines.map(_.asInstanceOf[VCFHeaderLine]).toSet.asJava)
    val schema = VCFSchemaInferrer.inferSchema(true, true, vcfHeader)
    val converter =
      new VariantContextToInternalRowConverter(vcfHeader, schema, ValidationStringency.STRICT)

    val vcb = new VariantContextBuilder("Unknown", "chr1", 1, 1, Seq(Allele.REF_A).asJava)
    val intArray = Array(1, 2)
    val strArray = Array("foo", "bar", "baz").map(UTF8String.fromString)
    val floatArray = Array(0.1, 1.2, 2.3, 3.4)

    if (arrToStr) {
      vcb.attribute(intArrayHeaderLine.getID, intArray.mkString(","))
      vcb.attribute(strArrayHeaderLine.getID, strArray.mkString(","))
      vcb.attribute(floatArrayHeaderLine.getID, floatArray.mkString(","))
    } else {
      vcb.attribute(intArrayHeaderLine.getID, intArray)
      vcb.attribute(strArrayHeaderLine.getID, strArray)
      vcb.attribute(floatArrayHeaderLine.getID, floatArray)
    }
    val vc = vcb.make

    val internalRow = converter.convertRow(vc, false)
    assert(
      internalRow
        .getArray(schema.fieldIndex("INFO_" + intArrayHeaderLine.getID))
        .toIntArray() sameElements intArray)
    assert(
      internalRow
        .getArray(schema.fieldIndex("INFO_" + strArrayHeaderLine.getID))
        .toObjectArray(StringType) sameElements strArray)
    assert(
      internalRow
        .getArray(schema.fieldIndex("INFO_" + floatArrayHeaderLine.getID))
        .toDoubleArray() sameElements floatArray)
  }
}
