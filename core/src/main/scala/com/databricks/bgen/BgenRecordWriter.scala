package com.databricks.bgen

import java.io.{BufferedOutputStream, ByteArrayOutputStream, DataOutput, OutputStream}
import java.nio.charset.StandardCharsets
import java.util.{Comparator, Arrays => JArrays, HashMap => JHashMap}
import java.util.zip.{Deflater, DeflaterOutputStream}

import com.google.common.io.{CountingOutputStream, LittleEndianDataOutputStream}
import org.apache.commons.math3.util.CombinatoricsUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import com.databricks.hls.common.HLSLogging
import com.databricks.vcf.BgenRow

/**
 * Writes BGEN records.
 *
 * @param outputStream The output stream to write to
 * @param schema Schema of the input rows
 * @param writeHeader If true, write the header
 * @param numVariants Number of variants (for the header)
 * @param bitsPerProb Number of bits used to represent each genotype/haplotype probability value
 * @param maxPloidy If ploidy is missing, it is inferred as some value in [1, maxPloidy]
 * @param defaultPloidy If phasing and ploidy are missing or the inferred ploidy is ambiguous, we
 *                      assume ploidy is defaultPloidy
 * @param defaultPhasing If phasing is missing and the inferred phasing is ambiguous, we assume
 *                       phasing is defaultPhasing
 */
class BgenRecordWriter(
    outputStream: OutputStream,
    schema: StructType,
    writeHeader: Boolean,
    numVariants: Long,
    bitsPerProb: Int,
    maxPloidy: Int,
    defaultPloidy: Int,
    defaultPhasing: Boolean)
    extends HLSLogging {

  import BgenRecordWriter._

  val converter = new InternalRowToBgenRowConverter(
    schema,
    maxPloidy,
    defaultPloidy,
    defaultPhasing
  )
  val stream = new LittleEndianDataOutputStream(outputStream)
  val probabilityWriter: (Long, LittleEndianDataOutputStream) => Unit = {
    bitsPerProb match {
      case 8 => (prob, ledos) => writeUnsignedByte(prob, ledos)
      case 16 => (prob, ledos) => writeUnsignedShort(prob, ledos)
      case 32 => (prob, ledos) => writeUnsignedInt(prob, ledos)
      case _ =>
        throw new IllegalArgumentException("Only probabilities of 8, 16, or 32 bits are accepted")
    }
  }
  val probBlockSizeMap = new JHashMap[GenotypeCharacteristics, ProbabilityBlockSize]()
  val buffSize = 8192

  var headerHasBeenWritten: Boolean = false

  private def shouldWriteHeader: Boolean = writeHeader && !headerHasBeenWritten

  // If header should be written but has not been written yet, write it
  private def maybeWriteHeader(sampleIds: Seq[Option[String]]): Unit = {
    if (shouldWriteHeader) {
      val headerWriter = new BgenHeaderWriter(stream, numVariants, sampleIds)
      headerWriter.writeHeader()
      headerHasBeenWritten = true
    }
  }

  private def maybeWriteHeaderForEmptyFile(): Unit = {
    if (shouldWriteHeader) {
      logger.info("Writing header for empty file")
      val headerWriter = new BgenHeaderWriter(stream, numVariants, Nil)
      headerWriter.writeHeader()
      headerHasBeenWritten = true
    }
  }

  /**
   * # Bytes | Description
   * ========+=====================================
   *    2    | Length of the variant ID = L_(id)
   * ----------------------------------------------
   *  L_(id) | The variant ID
   * ----------------------------------------------
   *    2    | Length of the rsid = L_(rsid)
   * ----------------------------------------------
   * L_(rsid)| The rsid
   * ----------------------------------------------
   *    2    | Length of the chromosome
   * ----------------------------------------------
   * L_(chr) | The chromosome
   * ----------------------------------------------
   *    4    | Variant position
   * ----------------------------------------------
   *    2    | Number of alleles = K
   * ----------------------------------------------
   *    4    | Length of the first allele = L_(a1)
   * ----------------------------------------------
   *  L_(a1) | The first allele
   * ----------------------------------------------
   *    4    | Length of the second allele = L_(a2)
   * ----------------------------------------------
   *  L_(a2) | The second allele
   * ----------------------------------------------
   *   ...   | ...
   * ----------------------------------------------
   *    4    | Length of the Kth allele = L_(aK)
   * ----------------------------------------------
   *  L_(aK) | The Kth allele
   * ----------------------------------------------
   */
  private def writeVariantIdentifyingBlock(row: BgenRow): Unit = {
    writeUTF8String(row.names.headOption.getOrElse(""), false, stream)
    writeUTF8String(row.names.lift(1).getOrElse(""), false, stream)
    writeUTF8String(row.contigName, false, stream)
    writeUnsignedInt(row.start.toInt + 1, stream)
    writeUnsignedShort(1 + row.alternateAlleles.length, stream) // Reference allele (1) + alts
    writeUTF8String(row.referenceAllele, true, stream)
    row.alternateAlleles.foreach { aa =>
      writeUTF8String(aa, true, stream)
    }
  }

  /**
   * # Bytes | Description
   * ========+======================================================================================
   *    4    | Number of samples
   * -----------------------------------------------------------------------------------------------
   *    2    | Number of alleles
   * -----------------------------------------------------------------------------------------------
   *    1    | Minimum ploidy of samples
   * -----------------------------------------------------------------------------------------------
   *    1    | Maximum ploidy of samples
   * -----------------------------------------------------------------------------------------------
   *    N    | The nth byte represents the ploidy and missingness of the nth sample. Ploidy is
   *         | encoded in the least significant 6 bits of this value. Missingness is encoded by the
   *         | most significant bit (1 if missing).
   * -----------------------------------------------------------------------------------------------
   *    1    | 1 if phased, 0 if unphased
   * -----------------------------------------------------------------------------------------------
   *    1    | Number of bits used to store each probability = B
   * -----------------------------------------------------------------------------------------------
   *    X    | Probabilities for each possible haplotype (if phased)/genotype (if unphased)
   * -----------------------------------------------------------------------------------------------
   */
  def getCompressedGenotypeProbabilityData(row: BgenRow): (Int, Array[Byte]) = {
    val baos = new ByteArrayOutputStream(buffSize)
    val dos = new DeflaterOutputStream(baos, new Deflater(), buffSize)
    val cos = new CountingOutputStream(dos)
    val bos = new BufferedOutputStream(cos, buffSize)
    val ledos = new LittleEndianDataOutputStream(bos)

    writeUnsignedInt(row.genotypes.length, ledos)
    val numAlleles = row.alternateAlleles.length + 1
    writeUnsignedShort(numAlleles, ledos)
    val (minPloidy, maxPloidy) = minMax(row.genotypes.map(_.ploidy.get), defaultPloidy)
    ledos.writeByte(minPloidy)
    ledos.writeByte(maxPloidy)

    var gtIdx = 0
    while (gtIdx < row.genotypes.length) {
      val gt = row.genotypes(gtIdx)
      val missingness = if (gt.posteriorProbabilities.isEmpty) 1 else 0
      ledos.writeByte(gt.ploidy.get + (missingness << 6))
      gtIdx += 1
    }

    val phased = if (row.genotypes.nonEmpty) row.genotypes.head.phased.get else defaultPhasing
    val phasedByte = if (phased) 1 else 0
    ledos.writeByte(phasedByte)

    ledos.writeByte(bitsPerProb)

    gtIdx = 0
    while (gtIdx < row.genotypes.length) {
      val gt = row.genotypes(gtIdx)

      val gc = GenotypeCharacteristics(numAlleles, phased, gt.ploidy.get)
      val probBlockSize = if (probBlockSizeMap.containsKey(gc)) {
        probBlockSizeMap.get(gc)
      } else {
        val probBlockSizeValue = getProbabilityBlockSize(gc)
        probBlockSizeMap.put(gc, probBlockSizeValue)
        probBlockSizeValue
      }

      val xList = if (gt.posteriorProbabilities.isEmpty) {
        // If missing probabilities, fill with 0's
        Array.fill(probBlockSize.probabilitiesPerBlock * probBlockSize.numBlocks)(0L).toSeq
      } else {
        calculateIntProbabilities(bitsPerProb, gt.posteriorProbabilities)
      }

      // For each probability block, write all but the last value
      var bi = 0
      var bpi = 0
      while (bi < probBlockSize.numBlocks) {
        while (bpi < probBlockSize.probabilitiesPerBlock - 1) {
          probabilityWriter(xList(bi * probBlockSize.probabilitiesPerBlock + bpi), ledos)
          bpi += 1
        }
        bpi = 0
        bi += 1
      }

      gtIdx += 1
    }

    ledos.close()
    cos.close()
    bos.close()
    dos.close()
    baos.close()
    (cos.getCount.toInt, baos.toByteArray)
  }

  /**
   * # Bytes | Description
   * ========+=============================================================================
   *    4    | Total length of the the rest of the data for this variant = C
   * --------------------------------------------------------------------------------------
   *    4    | Total length of the probability data after decompression = D
   * --------------------------------------------------------------------------------------
   *   C-4   | Genotype probability data for each of the N samples: decompressed to D bytes
   * --------------------------------------------------------------------------------------
   */
  def writeGenotypeDataBlock(row: BgenRow): Unit = {
    val (uncompressedLength, compressedGdp) = getCompressedGenotypeProbabilityData(row)

    // Uncompressed length is stored as an int (4 bytes)
    writeUnsignedInt(4 + compressedGdp.length, stream)
    writeUnsignedInt(uncompressedLength, stream)
    stream.write(compressedGdp)
  }

  /**
   * A Bgen file consists of a header followed by M variant data blocks.
   * Variant data blocks consist of:
   * - Variant identifying data
   * - Genotype data block
   */
  def write(row: InternalRow): Unit = {
    val bgenRow = converter.convert(row)

    maybeWriteHeader(bgenRow.genotypes.map(_.sampleId))
    writeVariantIdentifyingBlock(bgenRow)
    writeGenotypeDataBlock(bgenRow)
  }

  def close(): Unit = {
    maybeWriteHeaderForEmptyFile()
    stream.close()
    outputStream.close()
  }
}

object BgenRecordWriter {

  def minMax(a: Seq[Int], default: Int): (Int, Int) = {
    if (a.isEmpty) {
      (default, default)
    } else {
      a.foldLeft((a.head, a.head)) {
        case ((min, max), e) =>
          (math.min(min, e), math.max(max, e))
      }
    }
  }

  def writeUnsignedByte(l: Long, output: DataOutput): Unit = {
    output.writeByte(l.toInt)
  }

  def writeUnsignedShort(l: Long, output: DataOutput): Unit = {
    output.writeShort(l.toInt)
  }

  def writeUnsignedInt(l: Long, output: DataOutput): Unit = {
    output.writeInt(l.toInt)
  }

  def writeUTF8String(s: String, lengthAsInt: Boolean, output: DataOutput): Unit = {
    val byteArray = s.getBytes(StandardCharsets.UTF_8)
    if (lengthAsInt) {
      writeUnsignedInt(byteArray.length, output)
    } else {
      writeUnsignedShort(byteArray.length, output)
    }
    output.write(byteArray)
  }

  def getProbabilityBlockSize(gc: GenotypeCharacteristics): ProbabilityBlockSize = {
    if (gc.phased) {
      // One probability per allele per haplotype
      ProbabilityBlockSize(gc.numAlleles, gc.ploidy)
    } else {
      // One probability per possible genotype
      ProbabilityBlockSize(
        CombinatoricsUtils
          .binomialCoefficient(
            gc.ploidy + gc.numAlleles - 1,
            gc.numAlleles - 1
          )
          .toInt,
        1
      )
    }
  }

  /**
   * Given a vector v = (v_1, ... v_d) of d probabilities that sum to one, we find the integer
   * representation (where each int is stored in B bits) as follows:
   *
   * - Multiply v by 2**B-1.
   * - Compute the total fractional part F = sum_i (v_i - floor(v_i)).
   * - Form x by rounding the F entries of v with the largest fractional parts up to the nearest
   *   integer, and the other d-F entries down to the nearest smaller integer.
   */
  def calculateIntProbabilities(bitsPerProb: Int, probabilities: Seq[Double]): Seq[Long] = {
    val multiplier = (1L << bitsPerProb) - 1
    val numProbs = probabilities.length

    val vList = new Array[Double](numProbs)
    val fpIdxList = new Array[FractionalPartIndex](numProbs)
    var totalFractionalPart = 0d

    var i = 0
    while (i < numProbs) {
      val v = probabilities(i) * multiplier
      val fractionalPart = v - math.floor(v)

      vList(i) = v
      fpIdxList(i) = FractionalPartIndex(fractionalPart, i)
      totalFractionalPart += fractionalPart

      i += 1
    }

    JArrays.sort(fpIdxList, new SortByDescendingFractionalPart())

    val xList = new Array[Long](numProbs)
    i = 0
    for (i <- xList.indices) {
      xList(i) = if (fpIdxList(i).index < totalFractionalPart) {
        math.ceil(vList(i)).toLong
      } else {
        math.floor(vList(i)).toLong
      }
    }

    xList
  }
}

case class GenotypeCharacteristics(numAlleles: Int, phased: Boolean, ploidy: Int)
case class FractionalPartIndex(fractionalPart: Double, index: Int)
class SortByDescendingFractionalPart extends Comparator[FractionalPartIndex] {
  override def compare(a: FractionalPartIndex, b: FractionalPartIndex): Int = {
    val diff = b.fractionalPart - a.fractionalPart
    diff match {
      case d if d < 0 => -1
      case 0 => 0
      case _ => 1
    }
  }
}
case class ProbabilityBlockSize(probabilitiesPerBlock: Int, numBlocks: Int)
