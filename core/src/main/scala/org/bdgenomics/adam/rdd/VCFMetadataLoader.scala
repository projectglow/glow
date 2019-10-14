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

package org.bdgenomics.adam.rdd

import htsjdk.variant.vcf.VCFHeader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.seqdoop.hadoop_bam.util.{VCFHeaderReader, WrapSeekable}

object VCFMetadataLoader {

  // Currently private in ADAM (ADAMContext).
  def readVcfHeader(config: Configuration, pathName: String): VCFHeader = {
    val is = WrapSeekable.openPath(config, new Path(pathName))
    val header = VCFHeaderReader.readHeaderFrom(is)
    is.close()
    header
  }
}
