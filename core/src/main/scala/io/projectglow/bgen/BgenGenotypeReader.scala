package io.projectglow.bgen

import java.io.DataInput
import java.util.zip.Inflater

import com.github.luben.zstd.{Zstd => ZstdCodec}

trait BgenGenotypeReader {
  /**
   * Read genotypes from a bgen [[DataInput]] into a byte array. Returns the uncompressed genotype bytes.
   * Note that the genotypes returned are still encoded using the standard BGEN encoding scheme.
   */
  def readGenotypes(dataStream: DataInput): Array[Byte]
}

object BgenGenotypeReader {
  def fromCompressionType(typ: SnpBlockCompression): BgenGenotypeReader = typ match {
    case SnpBlockCompression.None => new UncompressedBgenGenotypeReader()
    case SnpBlockCompression.Zlib => new ZlibBgenGenotypeReader()
    case SnpBlockCompression.Zstd => new ZstdBgenGenotypeReader()
  }
}

class UncompressedBgenGenotypeReader extends BgenGenotypeReader {
  override def readGenotypes(dataStream: DataInput): Array[Byte] = {
    val genotypesLen = dataStream.readInt()
    val genotypeBytes = new Array[Byte](genotypesLen)
    dataStream.readFully(genotypeBytes)
    genotypeBytes
  }
}

class ZlibBgenGenotypeReader extends BgenGenotypeReader {
  override def readGenotypes(dataStream: DataInput): Array[Byte] = {
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
  override def readGenotypes(dataStream: DataInput): Array[Byte] = {
    val genotypesLen = dataStream.readInt() - 4
    val decompressedGenotypesLen = dataStream.readInt()
    val compressedBytes = new Array[Byte](genotypesLen)
    dataStream.readFully(compressedBytes)
    ZstdCodec.decompress(compressedBytes, decompressedGenotypesLen)
  }
}
