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

import java.io.DataInput
import java.util.zip.Inflater

import com.github.luben.zstd.{Zstd => ZstdCodec}

trait BgenGenotypeReader {

  /**
   * Read genotypes from a BGEN [[DataInput]] into a byte array. Returns the uncompressed genotype bytes.
   * Note that the genotypes returned are still encoded using the standard BGEN encoding scheme.
   *
   * Only layout type 2 is supported.
   */
  def readGenotypeBlock(dataStream: DataInput): Array[Byte]
}

object BgenGenotypeReader {
  def fromCompressionType(typ: SnpBlockCompression): BgenGenotypeReader = typ match {
    case SnpBlockCompression.None => new UncompressedBgenGenotypeReader()
    case SnpBlockCompression.Zlib => new ZlibBgenGenotypeReader()
    case SnpBlockCompression.Zstd => new ZstdBgenGenotypeReader()
  }
}

class UncompressedBgenGenotypeReader extends BgenGenotypeReader {
  override def readGenotypeBlock(dataStream: DataInput): Array[Byte] = {
    val genotypesLen = dataStream.readInt()
    val genotypeBytes = new Array[Byte](genotypesLen)
    dataStream.readFully(genotypeBytes)
    genotypeBytes
  }
}

class ZlibBgenGenotypeReader extends BgenGenotypeReader {
  override def readGenotypeBlock(dataStream: DataInput): Array[Byte] = {
    val genotypesLen = dataStream.readInt() - 4
    val decompressedGenotypesLen = dataStream.readInt()
    val compressedBytes = new Array[Byte](genotypesLen)
    val decompressedBytes = new Array[Byte](decompressedGenotypesLen)
    dataStream.readFully(compressedBytes)
    val inflater = new Inflater()
    inflater.setInput(compressedBytes)
    inflater.inflate(decompressedBytes)
    decompressedBytes
  }
}

class ZstdBgenGenotypeReader extends BgenGenotypeReader {
  override def readGenotypeBlock(dataStream: DataInput): Array[Byte] = {
    val genotypesLen = dataStream.readInt() - 4
    val decompressedGenotypesLen = dataStream.readInt()
    val compressedBytes = new Array[Byte](genotypesLen)
    dataStream.readFully(compressedBytes)
    ZstdCodec.decompress(compressedBytes, decompressedGenotypesLen)
  }
}
