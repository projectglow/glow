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

import java.io.OutputStream

import org.apache.hadoop.io.compress.{CompressionOutputStream, Compressor}
import org.seqdoop.hadoop_bam.util.{GlowBGZFOutputStream, BGZFCodec => HBBGZFCodec}

/**
 * A copy of Hadoop-BAM's BGZF codec that returns a Glow BGZF output stream.
 */
class BGZFCodec extends HBBGZFCodec {
  override def createOutputStream(out: OutputStream): CompressionOutputStream = {
    new GlowBGZFOutputStream(out)
  }

  override def createOutputStream(
      out: OutputStream,
      compressor: Compressor): CompressionOutputStream = {
    createOutputStream(out)
  }
}
