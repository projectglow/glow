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

import java.io.EOFException

import com.google.common.io.LittleEndianDataInputStream
import org.apache.hadoop.fs.FSDataInputStream

import io.projectglow.common.GlowLogging

/**
 * Parses genotype blocks of a BED file into an array. The iterator assumes that the input streams are currently at the
 * beginning of a genotype block.
 *
 * BED standard: https://www.cog-genomics.org/plink/1.9/formats#bed
 *
 * This class does not currently support the entire BED standard. Limitations:
 * - Only variant-major BEDs are supported.
 *
 * @param stream Data stream that records are read from. Must be little-endian.
 * @param underlyingStream Hadoop input stream that underlies the little-endian data stream. Only
 *                         used for cleaning up when there are no genotype blocks left.
 * @param numSamples The number of samples represented in each genotype block.
 * @param numBlocks The number of genotype blocks which can be read. `hasNext` will return `false` once we've reached
 *                  `numBlocks` blocks.
 * @param blockSize The size of a block in bytes; equal to `ceil(numSamples / 4)`
 */
class BedFileIterator(
    stream: LittleEndianDataInputStream,
    underlyingStream: FSDataInputStream,
    numSamples: Int,
    numBlocks: Int,
    blockSize: Int)
    extends Iterator[Array[Array[Int]]]
    with GlowLogging {

  var blockIdx = 0
  val callsArray: Array[Array[Int]] = new Array[Array[Int]](numSamples)
  val byteArray: Array[Byte] = new Array[Byte](blockSize)

  def hasNext(): Boolean = {
    val ret = blockIdx < numBlocks
    if (!ret) {
      cleanup()
    }
    ret
  }

  def next(): Array[Array[Int]] = {
    blockIdx += 1
    val bytesRead = stream.read(byteArray)

    if (bytesRead != blockSize) {
      cleanup()
      throw new EOFException(
        s"BED file corrupted: could not read block $blockIdx from $numBlocks blocks.")
    }

    var i = 0
    while (i < numSamples) {
      // Get the relevant 2 bits for the sample within the block
      callsArray(i) = twoBitsToCalls((byteArray(i / 4) >> (2 * (i % 4))) & 3)
      i += 1
    }
    callsArray
  }

  private def twoBitsToCalls(twoBits: Int): Array[Int] = {
    twoBits match {
      case 0 => Array(1, 1) // Homozygous for first (alternate) allele
      case 1 => Array(-1, -1) // Missing genotype
      case 2 => Array(0, 1) // Heterozygous
      case 3 => Array(0, 0) // Homozygous for second (reference) allele
    }
  }

  private def cleanup(): Unit = {
    underlyingStream.close()
  }
}
