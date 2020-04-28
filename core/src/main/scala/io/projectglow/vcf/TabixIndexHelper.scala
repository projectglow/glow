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

import java.io.File
import java.nio.file.Paths

import scala.collection.JavaConverters._

import com.google.common.annotations.VisibleForTesting
import htsjdk.tribble.index.tabix._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.{Filter, _}
import io.projectglow.common.GenomicIntervalUtils._
import htsjdk.samtools.util.Interval

import io.projectglow.common.{GlowLogging, WithUtils}

case class ParsedFilterResult(
    contig: Contig,
    startInterval: LongInterval,
    endInterval: LongInterval)

case class ContigAndInterval(contig: Contig, interval: LongInterval) extends Serializable {
  def toHTSJDKInterval: Interval = {
    if (contig.isSingleContig && interval.nonEmpty && interval.start > 0 && interval.end <= Int.MaxValue) {
      new Interval(contig.name, interval.start.toInt, interval.end.toInt)
    } else {
      throw new IllegalArgumentException("Need single contig, start >= 1, and end <= Int.MaxValue")
    }
  }
}

/** Contains filter parsing tools and other tools used to apply tabix index */
object TabixIndexHelper extends GlowLogging {

  /**
   * Parses filters provided by spark sql parser to generate the ParsedFilterResult=(contig,
   * startInterval, endInterval) that will be used to makeFilteredContigAndIntereval
   * The parser acts recursively to handle And and Or logical operators. Therefore it is able to
   * handle any nested combination of And and Or operators.
   * Not operator is not supported and if used its effect will be passed to spark filter parser.
   *
   * @param filters The Seq of filters provided by spark sql
   * @return A ParsedFilterResult = (contig: Contig, startInterval: LongInterval,
   *         endInterval: LongInterval)
   *
   *         contig: Is a Contig that indicates the chromosome identified by the filter
   *         [Note: (in the current implementation, only one single chromosome must result from
   *         the combination of all filters for the tabix index to be used]
   *         Contig.isEmpty will be true if combination of all filters result in an inconsistent chromosome
   *         Contig.isAnyContig will be true if combination of all filters does not specify a single
   *         chromosome or result in multiple chromosomes (tabix index use will be disabled by getFileRangeToRead)
   *
   *         startInterval: is a LongInterval denoting variant "start" range that is indicated
   *         by combination of all filters. LongInterval is 1-based but the 'start' column in
   *         filter is 0-based. The parser takes care of this conversion in all situations.
   *
   *         endInterval: is a LongInterval denoting variant "end" range that is indicated by
   *         combination of all filters.
   */

  @VisibleForTesting
  private[vcf] def parseFilter(filters: Seq[Filter]): ParsedFilterResult = {

    // contig and startInterval and endInterval get updated as conditions are parsed.

    var paramsOK: Boolean = true

    var contig = new Contig(true)
    var startInterval = new LongInterval(1, MAX_GENOME_COORDINATE)
    var endInterval = new LongInterval(1, MAX_GENOME_COORDINATE)

    for (x <- filters if paramsOK && contig.nonEmpty && startInterval.nonEmpty &&
      startInterval.nonEmpty) {
      x match {
        case And(left: Filter, right: Filter) =>
          // left and right are concatenated and parsed recursively
          val parsedLeftRight = parseFilter(Seq(left, right))

          // contig and start and end intervals are updated.
          contig = contig.intersect(parsedLeftRight.contig)
          startInterval = startInterval.intersect(parsedLeftRight.startInterval)
          endInterval = endInterval.intersect(parsedLeftRight.endInterval)

        case Or(left: Filter, right: Filter) =>
          // left and right are parsed individually and recursively.
          // Updating start and end intervals in the case of Or is not trivial intersection work.
          // There are two options here:
          // Option 1: Do Or action on left start and right start
          //           Do Or action on left end and right end
          //           Update start and end intervals individually using the And action with the
          //           respective intervals above
          //           Defer getting smallest query interval to getSmallestQueryInterval after
          //           all filters are parsed
          //
          // Option 2: Find the smallest query interval resulting from left and the smallest
          // query interval resulting from right
          //           Do Or action on them
          //           Update both start and end intervals to the same single interval resulting
          //           from the Or action
          //
          // Option 2 is more efficient than Option 1 and sometimes much more as it gets smallest
          // interval as soon as possible.
          //
          // Therefore, Option 2 is implemented here.

          val parsedLeft = parseFilter(Seq(left))
          val parsedRight = parseFilter(Seq(right))

          val orInterval =
            getSmallestQueryInterval(parsedLeft.startInterval, parsedLeft.endInterval).spanWith(
              getSmallestQueryInterval(parsedRight.startInterval, parsedRight.endInterval))

          contig = contig.intersect(parsedLeft.contig.union(parsedRight.contig))
          startInterval = startInterval.intersect(orInterval)
          endInterval = endInterval.intersect(orInterval)

        case EqualTo("contigName", value: String) =>
          contig = contig.intersect(new Contig(value))

        // Note that in all the following cases, LongInterval must be a 1-based interval
        // therefore the value in all 'start' cases is incremented by 1.
        case EqualTo(columnName: String, value: Long) =>
          if (columnName == "start") {
            if (value < 0 || value > MAX_GENOME_COORDINATE) {
              paramsOK = false
            } else {
              startInterval = startInterval.intersect(new LongInterval(value + 1, value + 1))
            }
          } else if (columnName == "end") {
            if (value < 1 || value > MAX_GENOME_COORDINATE) {
              paramsOK = false
            } else {
              endInterval = endInterval.intersect(new LongInterval(value, value))
            }
          }

        case GreaterThan(columnName: String, value: Long) =>
          if (columnName == "start") {
            if (value < -1 || value > MAX_GENOME_COORDINATE) {
              paramsOK = false
            } else {
              startInterval =
                startInterval.intersect(new LongInterval(value + 2, MAX_GENOME_COORDINATE))
            }
          } else if (columnName == "end") {
            if (value < 0 || value > MAX_GENOME_COORDINATE) {
              paramsOK = false
            } else {
              endInterval =
                endInterval.intersect(new LongInterval(value + 1, MAX_GENOME_COORDINATE))
            }
          }

        case GreaterThanOrEqual(columnName: String, value: Long) =>
          if (columnName == "start") {
            if (value < 0 || value > MAX_GENOME_COORDINATE) {
              paramsOK = false
            } else {
              startInterval =
                startInterval.intersect(new LongInterval(value + 1, MAX_GENOME_COORDINATE))
            }
          } else if (columnName == "end") {
            if (value < 1 || value > MAX_GENOME_COORDINATE) {
              paramsOK = false
            } else {
              endInterval = endInterval.intersect(new LongInterval(value, MAX_GENOME_COORDINATE))
            }
          }

        case LessThan(columnName: String, value: Long) =>
          if (columnName == "start") {
            if (value < 1 || value > MAX_GENOME_COORDINATE) {
              paramsOK = false
            } else {
              startInterval = startInterval.intersect(new LongInterval(1, value))
            } // starts at 1 because LongInterval must be 1-based interval.
          } else if (columnName == "end") {
            if (value < 2 || value > MAX_GENOME_COORDINATE) {
              paramsOK = false
            } else {
              endInterval = endInterval.intersect(new LongInterval(1, value - 1))
            }
          }

        case LessThanOrEqual(columnName: String, value: Long) =>
          if (columnName == "start") {
            if (value < 0 || value > MAX_GENOME_COORDINATE) {
              paramsOK = false
            } else {
              startInterval = startInterval.intersect(new LongInterval(1, value + 1))
            } // starts at 1 because LongInterval must be 1-based interval.
          } else if (columnName == "end") {
            if (value < 1 || value > MAX_GENOME_COORDINATE) {
              paramsOK = false
            } else {
              endInterval = endInterval.intersect(new LongInterval(1, value))
            }
          }

        case _ =>
      }
    }

    if (!paramsOK) {
      ParsedFilterResult(
        new Contig(true),
        new LongInterval(1, MAX_GENOME_COORDINATE),
        new LongInterval(1, MAX_GENOME_COORDINATE))
    } else {
      ParsedFilterResult(contig, startInterval, endInterval)
    }
  }

  /**
   * Given a startInterval and an endInterval returns the Smallest Query Interval (SQI)
   * on the contig to be checked for overlap in iterator's overlapdetector (and/or)
   * queried in the tabix index.
   * SQI has overlap with all variants that have "start" in startInterval and "end" in endInterval.
   * [Note: Tabix index works based on returning any bins that have overlap with the query
   * interval.
   * Therefore, it is in the interest of performance to find the smallest query interval
   * that has overlap with all variants resulting from the filters.
   * If SQI is queried in the tabix, all the bins containing any variant that overlaps with this
   * interval will be returned, guaranteeing that we do not miss any filtered variant.
   * Being the smallest query interval, we make sure to minimize the number of false positives.
   *
   * Some Examples:
   *
   * startInterval
   * ---------------++++++++++++++++++++++++--------------
   * endInterval
   * ++++++++++++++++++++---------------------------------
   * SQI
   * ---------------+++++---------------------------------
   *
   * ***************************************
   *
   * startInterval
   * --++++++++++++---------------------------------------
   * endInterval
   * -----------------------++++++++++++++++++++----------
   * SQI
   * -----------------------+-----------------------------
   *
   * ****************************************
   *
   * @param startInterval
   * @param endInterval
   * @return SQI
   */

  @VisibleForTesting
  private[vcf] def getSmallestQueryInterval(
      startInterval: LongInterval,
      endInterval: LongInterval): LongInterval = {
    if (startInterval.nonEmpty && endInterval.nonEmpty) {
      if (startInterval.start > endInterval.end) {
        new LongInterval()
      } else if (startInterval.overlaps(endInterval)) {
        startInterval.intersect(endInterval)
      } else {
        new LongInterval(endInterval.start, endInterval.start)
      }
    } else {
      new LongInterval()
    }
  }

  /** Getting Seq of filters, returns a ContigAndInterval representing contig and SQI,
   * and takes care of useFilterParser option.
   * @param filters
   * @return ContigAndInterval indicating contig and SQI of the filtered interval
   */
  def makeFilteredContigAndInterval(
      filters: Seq[Filter],
      useFilterParser: Boolean,
      useIndex: Boolean): ContigAndInterval = {

    if (useFilterParser) {
      val parsedFilterResult = parseFilter(filters)
      ContigAndInterval(
        parsedFilterResult.contig,
        getSmallestQueryInterval(
          parsedFilterResult.startInterval,
          parsedFilterResult.endInterval
        )
      )
    } else {
      if (useIndex) {
        logger.info("Error: Filter parser is deactivated while requesting index use.")
        throw new IllegalArgumentException
      }
      ContigAndInterval(
        new Contig(true),
        new LongInterval(1, MAX_GENOME_COORDINATE)
      )
    }
  }

  /**
   * Performs all the checks needed to use tabix index
   * If a check fails generates appropriate message
   * Otherwise generates block range by querying tabix index based
   * on the filteredContigAndInterval produces by makeFilteredContigAndIntereval.
   */
  def getFileRangeToRead(
      hadoopFs: FileSystem,
      file: PartitionedFile,
      conf: Configuration,
      hasFilter: Boolean, // true if user did specify a filter
      useIndex: Boolean,
      filteredContigAndInterval: ContigAndInterval): Option[(Long, Long)] = {

    // Do not read index files
    if (file.filePath.endsWith(VCFFileFormat.INDEX_SUFFIX)) {
      return None
    }

    val path = new Path(file.filePath)
    val indexFile = new Path(file.filePath + VCFFileFormat.INDEX_SUFFIX)
    val isGzip = VCFFileFormat.isGzip(file, conf)

    if (filteredContigAndInterval.contig.nonEmpty && filteredContigAndInterval.interval.nonEmpty) {
      if (isGzip && file.start == 0) {
        logger.info("Reading gzip file from beginning to end")
        val fileLength = hadoopFs.getFileStatus(path).getLen
        Some((0, fileLength))
      } else if (isGzip) {
        logger.info("Skipping gzip file because task starts in the middle of the file")
        None
      } else if (!hasFilter) {
        Some((file.start, file.start + file.length))
      } else if (!useIndex) {
        logger.info("Tabix index use disabled by the user...")
        Some((file.start, file.start + file.length))
      } else if (!VCFFileFormat.isValidBGZ(path, conf)) {
        logger.info("The file is not bgzipped... not using tabix index...")
        Some((file.start, file.start + file.length))
      } else if (filteredContigAndInterval.contig.isAnyContig) {
        logger
          .info(
            "More than one chromosome or chromosome number not provided " +
            "in the filter... will not use tabix index..."
          )
        Some((file.start, file.start + file.length))
      } else if (!hadoopFs.exists(indexFile)) {
        logger.info("Did not find tabix index file ...")
        Some((file.start, file.start + file.length))
      } else {
        logger.info(s"Found tabix index file ${indexFile} for VCF file ${file.filePath}")
        val localIdxPath = downloadTabixIfNecessary(hadoopFs, indexFile)
        val localIdxFile = new File(localIdxPath)
        val tabixIdx = new TabixIndex(localIdxFile)
        val offsetList = tabixIdx
          .getBlocks(
            filteredContigAndInterval.contig.name,
            filteredContigAndInterval.interval.start.toInt,
            filteredContigAndInterval.interval.end.toInt
          )
          .asScala
          .toList

        if (offsetList.isEmpty) {
          None
        } else {
          val (minOverBlocks, maxOverBlocks) = offsetList
            .foldLeft((offsetList(0).getStartPosition, offsetList(0).getEndPosition)) {
              case ((myStart, myEnd), e) =>
                (Math.min(myStart, e.getStartPosition), Math.max(myEnd, e.getEndPosition))
            }
          val blockRangeStart = Math.max(file.start, minOverBlocks >> 16) // Shift 16 bits to get
          // file offset of the bin from bgzipped virtual file offset.
          val blockRangeEnd = Math.min(file.start + file.length, (maxOverBlocks >> 16) + 0xFFFF)
          // 0xFFFF is the maximum possible length of an uncompressed bin.
          if (blockRangeStart <= blockRangeEnd) {
            Some((blockRangeStart, blockRangeEnd))
          } else {
            None
          }
        }
      }
    } else {
      logger.info(
        "Filter parser indicates no rows satisfy the filters... no need to use tabix Index..."
      )
      None

    }
  }

  private def downloadTabixIfNecessary(hadoopFs: FileSystem, path: Path): String = {
    val localDir = Paths.get(System.getProperty("java.io.tmpdir")).resolve("tabix_indices").toFile
    localDir.mkdirs()

    val localPath = s"$localDir/${path.getName.replaceAllLiterally("/", "__")}"
    WithUtils.withLock(VCFFileFormat.idxLock.get(path)) {
      if (!new File(localPath).exists()) {
        val myPath = new Path(localPath)
        hadoopFs.copyToLocalFile(path, myPath)
      }
    }

    localPath
  }

  @VisibleForTesting
  private[vcf] val MAX_GENOME_COORDINATE: Long = Int.MaxValue.toLong

}
