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

package io.projectglow.bgen

import com.google.common.io.LittleEndianDataOutputStream

private[bgen] class BgenHeaderWriter(
    ledos: LittleEndianDataOutputStream,
    numVariants: Long,
    sampleIds: Seq[Option[String]]) {

  import BgenRecordWriter._

  private val HEADER_BLOCK_LENGTH = 20
  private val COMPRESSION_TYPE = 1 // zlib
  private val LAYOUT_TYPE = 2
  private val LAYOUT_OFFSET = 2
  private val SAMPLE_OFFSET = 31
  private val hasSampleIds: Boolean = sampleIds.nonEmpty && sampleIds.forall(_.isDefined)
  private val sampleIdBlockLength: Int = {
    if (hasSampleIds) {
      // We assume that the characters are ASCII (one byte in UTF-8)
      8 + 2 * sampleIds.length + sampleIds.map(_.get.length).sum
    } else {
      0
    }
  }

  /**
   *   Bit  | Value | Description
   * =======+=======+============================================
   *   0-1  |   1   | SNP blocks compressed using zlib
   * ------------------------------------------------------------
   *   2-5  |   2   | SNP blocks laid out according to Layout 2
   * ------------------------------------------------------------
   *   31   |  0/1  | 1 if sample IDs stored in file, 0 otherwise
   * ------------------------------------------------------------
   */
  private def writeFlags(): Unit = {
    val hasSampleIdsFlag = if (hasSampleIds) 1 else 0
    val flags = COMPRESSION_TYPE +
      (LAYOUT_TYPE << LAYOUT_OFFSET) +
      (hasSampleIdsFlag << SAMPLE_OFFSET)
    ledos.writeInt(flags)
  }

  /**
   * # Bytes | Description
   * ========+=================================
   *    4    | Length of the header block = L_H
   * ------------------------------------------
   *    4    | Number of variants
   * ------------------------------------------
   *    4    | Number of samples
   * ------------------------------------------
   *    4    | Magic numbers (b, g, e, n)
   * ------------------------------------------
   *    0    | Free data
   * ------------------------------------------
   *    4    | Flags
   * ------------------------------------------
   */
  private def writeHeaderBlock(): Unit = {
    writeUnsignedInt(HEADER_BLOCK_LENGTH, ledos)
    writeUnsignedInt(numVariants.toInt, ledos)
    writeUnsignedInt(sampleIds.length, ledos)
    // Write magic numbers
    ledos.writeByte('b')
    ledos.writeByte('g')
    ledos.writeByte('e')
    ledos.writeByte('n')
    writeFlags()
  }

  /**
   * # Bytes | Description
   * ========+==================================
   *    4    | Length of bytes in block = L_(SI)
   * -------------------------------------------
   *    4    | Number of samples = N
   * -------------------------------------------
   *    2    | Length of sample ID 1
   * -------------------------------------------
   *  L_(s1) | Sample ID 1
   * -------------------------------------------
   *    2    | Length of sample ID 2
   * -------------------------------------------
   *  L_(s2) | Sample ID 2
   * -------------------------------------------
   *   ...   | ...
   * -------------------------------------------
   *    2    | Length of sample ID N
   * -------------------------------------------
   *  L_(sN) | Sample ID N
   * -------------------------------------------
   */
  private def maybeWriteSampleIdBlock(): Unit = {
    if (hasSampleIds) {
      writeUnsignedInt(sampleIdBlockLength, ledos)
      writeUnsignedInt(sampleIds.length, ledos)
      sampleIds.foreach { sid =>
        writeUTF8String(sid.get, false, ledos)
      }
    }
  }

  /**
   * # Bytes | Description
   * ========+=========================================================
   *    4    | Offset of the first byte of the first variant data block
   * ------------------------------------------------------------------
   *   L_H   | Header block
   * ------------------------------------------------------------------
   *  L_(SI) | Sampler identifier block (if present)
   * ------------------------------------------------------------------
   */
  def writeHeader(): Unit = {
    writeUnsignedInt(HEADER_BLOCK_LENGTH + sampleIdBlockLength, ledos)
    writeHeaderBlock()
    maybeWriteSampleIdBlock()
  }
}
