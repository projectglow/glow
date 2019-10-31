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

package io.projectglow.plink

import com.google.common.io.LittleEndianDataInputStream
import org.apache.hadoop.fs.FSDataInputStream

/**
 * Parses genotype blocks of a BED file. The iterator assumes that the input streams are currently at the beginning of a
 * genotype block.
 *
 * BED standard: https://www.cog-genomics.org/plink/1.9/formats#bed
 *
 * This class does not currently support the entire BED standard. Limitations:
 * - Only variant-major BEDs are supported.
 *
 * @param stream Data stream that records are read from. Must be little-endian.
 * @param underlyingStream Hadoop input stream that underlies the little-endian data stream. Only
 *                         used for cleaning up when there are no genotype blocks left.
 * @param numBlocks The number of genotype blocks to be read. `hasNext` will return `false` once we've read `numBlocks`
 *                  blocks.
 * @param blockSize The size of a block in bytes; equal to `ceil(numSamples / 4)`
 */
class BedFileIterator(
    stream: LittleEndianDataInputStream,
    underlyingStream: FSDataInputStream,
    numBlocks: Int,
    blockSize: Int)
    extends Iterator[Array[Byte]] {

  var blockIdx = 0
  val byteArray: Array[Byte] = new Array[Byte](blockSize)

  def hasNext(): Boolean = {
    val ret = blockIdx < numBlocks
    if (!ret) {
      cleanup()
    }
    ret
  }

  def next(): Array[Byte] = {
    blockIdx += 1
    stream.readFully(byteArray)
    byteArray
  }

  private def cleanup(): Unit = {
    underlyingStream.close()
  }
}
