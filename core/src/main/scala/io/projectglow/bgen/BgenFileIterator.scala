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

import java.io.{ByteArrayInputStream, DataInput, DataInputStream}
import java.nio.charset.StandardCharsets
import java.util.zip.Inflater

import com.google.common.io.LittleEndianDataInputStream
import org.apache.commons.math3.util.CombinatoricsUtils
import org.apache.hadoop.fs.FSDataInputStream

import io.projectglow.common.{BgenGenotype, BgenRow, GlowLogging}

/**
 * Parses variant records of a BGEN file into the [[io.projectglow.common.VCFRow]] schema. The iterator assumes that the
 * input streams are currently at the beginning of a variant block.
 *
 * The `init` method should be called before reading variants to skip to an appropriate starting
 * point.
 *
 * BGEN standard: https://www.well.ox.ac.uk/~gav/bgen_format/
 *
 * This class does not currently support the entire BGEN standard. Limitations:
 * - Only layout version 2 is supported
 * - Only zlib compressions is supported
 * - Only 8, 16, and 32 bit probabilities are supported
 *
 * @param metadata BGEN header info
 * @param stream Data stream that records are read from. Must be little-endian.
 * @param underlyingStream Hadoop input stream that underlies the little-endian data stream. Only
 *                         used for 1) finding the current stream position 2) cleaning up when there
 *                         are no variants left
 * @param minPos The minimum stream position from which variant blocks can be read.
 * @param maxPos The maximum stream position from which variant blocks can be read. `hasNext` will
 *               return `false` once we've reached this position.
 */
private[projectglow] class BgenFileIterator(
    metadata: BgenMetadata,
    stream: LittleEndianDataInputStream,
    underlyingStream: FSDataInputStream,
    minPos: Long,
    maxPos: Long)
    extends Iterator[BgenRow]
    with GlowLogging {

  import BgenFileIterator._

  def init(): Unit = {
    while (underlyingStream.getPos < minPos) {
      skipVariantBlock()
    }
  }

  def hasNext(): Boolean = {
    val ret = underlyingStream.getPos < maxPos
    if (!ret) {
      cleanup()
    }
    ret
  }

  def next(): BgenRow = {
    val variantId = readUTF8String(stream)
    val rsid = readUTF8String(stream)
    val contigName = readUTF8String(stream)
    val start = Integer.toUnsignedLong(stream.readInt()) - 1
    val nAlleles = stream.readUnsignedShort()
    val alleles = (1 to nAlleles).map(_ => readUTF8String(stream, lengthAsInt = true))

    val genotypesLen = stream.readInt() - 4
    val uncompressedGenotypesLen = stream.readInt()
    val compressedBytes = new Array[Byte](genotypesLen)
    val uncompressedBytes = new Array[Byte](uncompressedGenotypesLen)

    // Uncompress probability info
    stream.readFully(compressedBytes)
    val inflater = new Inflater()
    inflater.setInput(compressedBytes)
    inflater.inflate(uncompressedBytes)

    val rawGenotypeStream = new DataInputStream(new ByteArrayInputStream(uncompressedBytes))
    val genotypeStream = new LittleEndianDataInputStream(rawGenotypeStream)
    val genotypes = readGenotypes(nAlleles, genotypeStream, metadata.sampleIds)

    BgenRow(
      contigName,
      start,
      start + alleles.head.length,
      Seq(variantId, rsid),
      alleles.head,
      alleles.tail,
      genotypes
    )
  }

  /**
   * Cheaply skip over a variant block while reading as little data as possible.
   */
  private def skipVariantBlock(): Unit = {
    skipString() // variant id
    skipString() // rsid
    skipString() // contigName
    stream.readInt() // start
    val nAlleles = stream.readUnsignedShort()
    (1 to nAlleles).foreach { _ =>
      skipString(lengthAsInt = true)
    } // alleles
    val probabilityBlockSize = Integer.toUnsignedLong(stream.readInt())
    stream.skip(probabilityBlockSize) // probabilities
  }

  private def readGenotypes(
      nAllelesFromVariant: Int,
      genotypeStream: LittleEndianDataInputStream,
      sampleIds: Option[Array[String]]): Seq[BgenGenotype] = {
    val nSamples = genotypeStream.readInt()
    val nAlleles = genotypeStream.readUnsignedShort()
    if (nAlleles != nAllelesFromVariant) {
      logger.warn(
        s"Number of alleles in genotype block did not match variant identifier block " +
        s"($nAlleles vs $nAllelesFromVariant). Using value from genotype block."
      )
    }
    val minPloidy = genotypeStream.readUnsignedByte()
    val maxPloidy = genotypeStream.readUnsignedByte()
    val ploidyWithMissingnessBySample = new Array[Int](nSamples)
    var i = 0
    while (i < nSamples) {
      ploidyWithMissingnessBySample(i) = genotypeStream.readUnsignedByte()
      i += 1
    }

    val phasedByte = genotypeStream.readUnsignedByte()
    val phased = phasedByte == 1
    val bitsPerProbability = genotypeStream.readUnsignedByte()

    if (phased) {
      readPhasedGenotypes(
        genotypeStream,
        nAlleles,
        ploidyWithMissingnessBySample,
        bitsPerProbability,
        sampleIds
      )
    } else {
      readUnphasedGenotypes(
        genotypeStream,
        nAlleles,
        ploidyWithMissingnessBySample,
        bitsPerProbability,
        sampleIds
      )
    }
  }

  /**
   * Read phased genotype probabilities. Consult the linked standard doc for complete information
   * about how probabilities are stored.
   *
   * Tl;dr:
   * - Each probability is stored as an int in [0, 2 ** bitsPerProbability). You divide by
   * 2 ** bitsPerProbability - 1 to get the probability as a double
   * - For each sample, there are ploidy * nAlleles probabilities
   * - The last allele probability for each haplotype is omitted since it can be inferred from
   * the others
   * @return An array of probabilities for each sample
   */
  private def readPhasedGenotypes(
      probStream: DataInput,
      nAlleles: Int,
      ploidyBySample: Seq[Int],
      bitsPerProbability: Int,
      sampleIds: Option[Array[String]]): Array[BgenGenotype] = {
    val output = new Array[BgenGenotype](ploidyBySample.size)
    val divisor = (1 << bitsPerProbability) - 1
    var i = 0
    var j = 0
    var k = 0
    while (i < ploidyBySample.length) {
      j = 0

      val ploidyWithMissingness = ploidyBySample(i)
      val ploidy = ploidyWithMissingness & 63
      val numValues = ploidy * nAlleles
      val probabilityBuffer = new Array[Double](if (ploidyWithMissingness > 63) 0 else numValues)

      while (j < ploidy) {
        k = 0
        var sum = 0L
        while (k < nAlleles - 1) {
          val nextInt = readProbability(probStream, bitsPerProbability)
          sum += nextInt
          writeProbability(nextInt, divisor, j * nAlleles + k, probabilityBuffer)

          k += 1
        }
        writeProbability(divisor - sum, divisor, j * nAlleles + k, probabilityBuffer)

        j += 1
      }

      val sampleId = if (sampleIds.isDefined) Option(sampleIds.get(i)) else None
      output(i) = BgenGenotype(sampleId, Option(true), Option(ploidy), probabilityBuffer)

      i += 1
    }
    output
  }

  /**
   * Read unphased genotype probabilities. Consult the linked standard doc for complete information
   * about how probabilities are stored.
   *
   * Tl;dr:
   * - Each probability is stored as an int in [0, 2 ** bitsPerProbability). You divide by
   * 2 ** bitsPerProbability - 1 to get the probability as a double
   * - For each sample, there are alleles multichoose ploidy probabilties
   * - The last probability for each sample is omitted since it can be inferred from
   * the others
   * @return An array of probabilities for each sample
   */
  private def readUnphasedGenotypes(
      probStream: DataInput,
      nAlleles: Int,
      ploidyBySample: Seq[Int],
      bitsPerProbability: Int,
      sampleIds: Option[Array[String]]): Array[BgenGenotype] = {
    val output = new Array[BgenGenotype](ploidyBySample.size)
    val divisor = (1L << bitsPerProbability) - 1
    var i = 0
    while (i < ploidyBySample.length) {
      var j = 0

      val ploidyWithMissingness = ploidyBySample(i)
      val ploidy = ploidyWithMissingness & 63
      val nCombs = CombinatoricsUtils.binomialCoefficient(ploidy + nAlleles - 1, ploidy).toInt
      val probabilityBuffer = new Array[Double](if (ploidyWithMissingness > 63) 0 else nCombs)

      var sum = 0L
      while (j < nCombs - 1) {
        val nextInt = readProbability(probStream, bitsPerProbability)
        sum += nextInt
        writeProbability(nextInt, divisor, j, probabilityBuffer)
        j += 1
      }
      writeProbability(divisor - sum, divisor, j, probabilityBuffer)
      val sampleId = if (sampleIds.isDefined) Option(sampleIds.get(i)) else None
      output(i) = BgenGenotype(sampleId, Option(false), Option(ploidy), probabilityBuffer)

      i += 1
    }
    output
  }

  private def readProbability(inputStream: DataInput, bits: Int): Long = bits match {
    case 8 => inputStream.readUnsignedByte()
    case 16 => inputStream.readUnsignedShort()
    case 32 => Integer.toUnsignedLong(inputStream.readInt())
    case _ =>
      throw new IllegalArgumentException("Only probabilities of 8, 16, or 32 bits are accepted")
  }

  /**
   * Writes the probability to the output array while handling the case where the output array
   * is empty (for missing samples).
   */
  private def writeProbability(p: Long, divisor: Long, idx: Int, outputArr: Array[Double]): Unit = {
    if (outputArr.length == 0) {
      return
    }

    outputArr(idx) = p.toDouble / divisor
  }

  private def cleanup(): Unit = {
    underlyingStream.close()
  }

  private def skipString(lengthAsInt: Boolean = false): Unit = {
    val len = if (lengthAsInt) {
      stream.readInt()
    } else {
      stream.readUnsignedShort()
    }
    stream.skipBytes(len)
  }
}

private[projectglow] object BgenFileIterator {

  /**
   * Utility function to read a UTF8 string from a data stream. Included in the companion object
   * so that it can be used by the header reader and the file iterator.
   */
  def readUTF8String(stream: DataInput, lengthAsInt: Boolean = false): String = {
    val len = if (lengthAsInt) {
      stream.readInt()
    } else {
      stream.readUnsignedShort()
    }
    val bytes = new Array[Byte](len)
    stream.readFully(bytes)
    new String(bytes, StandardCharsets.UTF_8)
  }
}

/**
 * Read a BGEN header from a data stream. Performs basic validation on the header parameters
 * according to what the reader currently supports.
 */
private[projectglow] class BgenHeaderReader(stream: LittleEndianDataInputStream)
    extends GlowLogging {

  def readHeader(sampleIdsOpt: Option[Seq[String]] = None): BgenMetadata = {
    val variantOffset = Integer.toUnsignedLong(stream.readInt()) + 4
    val headerLength = Integer.toUnsignedLong(stream.readInt())
    val nVariantBlocks = Integer.toUnsignedLong(stream.readInt())
    val nSamples = Integer.toUnsignedLong(stream.readInt())
    val magicNumber = (1 to 4).map(n => stream.readByte())

    require(
      magicNumber == Seq('b', 'g', 'e', 'n') || magicNumber == Seq(0, 0, 0, 0),
      s"Magic bytes were neither 'b', 'g', 'e', 'n' nor 0, 0, 0, 0 ($magicNumber)"
    )

    val freeData = new Array[Byte](headerLength.toInt - 20)
    stream.readFully(freeData)

    val flags = stream.readInt()
    val compressionType = flags & 3
    require(compressionType == 1, "Only zlib compression is supported")
    val layoutType = flags >> 2 & 15
    val hasSampleIds = flags >> 31 & 1

    val base = BgenMetadata(variantOffset, nSamples, nVariantBlocks, layoutType, Zlib, None)

    if (hasSampleIds == 1) {
      addSampleIdsFromHeader(headerLength, base)
    } else if (hasSampleIds == 0 && sampleIdsOpt.isDefined) {
      logger.warn("No sample IDs were parsed from the BGEN or .sample file.")
      addSampleIdsFromFile(sampleIdsOpt.get, base)
    } else {
      base
    }
  }

  private def addSampleIdsFromHeader(headerLength: Long, base: BgenMetadata): BgenMetadata = {
    val sampleBlockLength = Integer.toUnsignedLong(stream.readInt())
    val numSamples = Integer.toUnsignedLong(stream.readInt())

    if (numSamples != base.nSamples) {
      logger.warn(
        s"BGEN number of samples in sample ID block does not match header. " +
        s"($numSamples != ${base.nSamples})"
      )
    }
    if (sampleBlockLength + headerLength >= base.firstVariantOffset) {
      logger.warn(
        s"BGEN sample block length + header length >= first variant offset. File " +
        s"appears to be malformed, but attempting to parse anyway. " +
        s"($sampleBlockLength + $headerLength >= ${base.firstVariantOffset})"
      )
    }

    base.copy(sampleIds = Option((1 to numSamples.toInt).map { _ =>
      BgenFileIterator.readUTF8String(stream)
    }.toArray))
  }

  private def addSampleIdsFromFile(sampleIds: Seq[String], base: BgenMetadata): BgenMetadata = {
    val numSamples = sampleIds.length

    if (numSamples != base.nSamples) {
      logger.warn(
        s"BGEN number of samples in .sample file does not match header. " +
        s"($numSamples != ${base.nSamples})"
      )
    }

    base.copy(sampleIds = Some(sampleIds.toArray))
  }
}

private[projectglow] case class BgenMetadata(
    firstVariantOffset: Long,
    nSamples: Long,
    nVariantBlocks: Long,
    layoutType: Int,
    compressionType: SNPBlockCompression,
    sampleIds: Option[Array[String]])

sealed trait SNPBlockCompression
case object Zlib extends SNPBlockCompression
case object Zstd extends SNPBlockCompression
