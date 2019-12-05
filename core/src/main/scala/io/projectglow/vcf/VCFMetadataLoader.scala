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

import htsjdk.variant.vcf.VCFHeader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.seqdoop.hadoop_bam.util.{VCFHeaderReader, WrapSeekable}

import io.projectglow.common.WithUtils

object VCFMetadataLoader {

  // From ADAMContext, see:
  // https://github.com/bigdatagenomics/adam/blob/master/adam-core/src/main/scala/org/bdgenomics/adam/rdd/ADAMContext.scala
  def readVcfHeader(config: Configuration, pathName: String): VCFHeader = {
    WithUtils.withCloseable(WrapSeekable.openPath(config, new Path(pathName))) { is =>
      VCFHeaderReader.readHeaderFrom(is)
    }
  }
}
