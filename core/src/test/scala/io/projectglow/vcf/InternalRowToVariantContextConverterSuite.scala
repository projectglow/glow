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
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import io.projectglow.sql.GlowBaseTest

class InternalRowToVariantContextConverterSuite extends GlowBaseTest {
  lazy val NA12878 = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val header = VCFMetadataLoader.readVcfHeader(sparkContext.hadoopConfiguration, NA12878)
  lazy val headerLines = header.getMetaDataInInputOrder.asScala.toSet

  private val optionsSeq = Seq(
    Map("flattenInfoFields" -> "true", "includeSampleIds" -> "true"),
    Map("flattenInfoFields" -> "true", "includeSampleIds" -> "false"),
    Map("flattenInfoFields" -> "false", "includeSampleIds" -> "false"),
    Map("splitToBiallelic" -> "true", "includeSampleIds" -> "true")
  )

  gridTest("common schema options pass strict validation")(optionsSeq) { options =>
    val df = spark.read.format("vcf").options(options).load(NA12878)
    new InternalRowToVariantContextConverter(
      toggleNullability(df.schema, true),
      headerLines,
      false,
      ValidationStringency.STRICT).validate()

    new InternalRowToVariantContextConverter(
      toggleNullability(df.schema, false),
      headerLines,
      false,
      ValidationStringency.STRICT).validate()
  }

  private def toggleNullability[T <: DataType](dt: T, nullable: Boolean): T = dt match {
    case at: ArrayType => at.copy(containsNull = nullable).asInstanceOf[T]
    case st: StructType =>
      val fields = st.map { f =>
        f.copy(dataType = toggleNullability(f.dataType, nullable), nullable = nullable)
      }
      StructType(fields).asInstanceOf[T]
    case mt: MapType => mt.copy(valueContainsNull = nullable).asInstanceOf[T]
    case other => other
  }
}
