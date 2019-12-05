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
import htsjdk.variant.variantcontext.{VariantContext => HtsjdkVariantContext}
import htsjdk.variant.vcf.VCFHeader

import io.projectglow.common.VCFRow

/**
 * VCFRow -> HTSJDK VariantContext
 * Under the hood, this class relies on a [[InternalRowToVariantContextConverter]].
 */
class VCFRowToVariantContextConverter(
    vcfHeader: VCFHeader,
    replaceMissingSampleIds: Boolean,
    stringency: ValidationStringency = ValidationStringency.LENIENT)
    extends Serializable {

  // Encoders are not thread safe, so make a copy here
  private val vcfRowEncoder = VCFRow.encoder.copy()
  private val internalRowConverter =
    new InternalRowToVariantContextConverter(
      VCFRow.schema,
      vcfHeader.getMetaDataInInputOrder.asScala.toSet,
      replaceMissingSampleIds,
      stringency)

  def convert(vcfRow: VCFRow): HtsjdkVariantContext = {
    internalRowConverter
      .convert(vcfRowEncoder.toRow(vcfRow))
      .getOrElse(throw new IllegalStateException(s"Could not convert VCFRow $vcfRow"))
  }
}
