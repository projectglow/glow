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
import io.projectglow.common.{GlowLogging, SimpleInterval, WithUtils}

/** An extended Contig class used by filter parser that keeps an Option(contigName)
 * updated under And and Or operations and provides other required functionalities
 */
class FilterContig(contigName: String) {
  private var contig: Option[String] = Option(contigName)

  def actionAnd(other: FilterContig): FilterContig = {
    (contig, other.getContigName) match {
      case (Some(i), Some(j)) =>
        contig = if (i == j || j.isEmpty) {
          Option(i)
        } else if (i.isEmpty) {
          Option(j)
        } else {
          None
        }
      case (_, None) => contig = None
      case (None, _) =>
    }
    this
  }

  def actionOr(other: FilterContig): FilterContig = {
    (contig, other.getContigName) match {
      case (Some(i), Some(j)) => contig = if (i == j) Option(i) else Option("")
      case (Some(i), None) => contig = Option(i)
      case (None, Some(j)) => contig = Option(j)
      case (None, None) =>
    }
    this
  }

  def isDefined: Boolean = contig.isDefined

  def isEmpty: Boolean = contig.isEmpty

  def getContigName: Option[String] = contig

  def isSame(other: FilterContig): Boolean = {
    (contig, other.getContigName) match {
      case (Some(i), Some(j)) => i == j
      case (None, _) => other.isEmpty
      case (_, None) => contig.isEmpty
    }
  }

}

/** An extended 1-based Interval class used by filter parser that keeps an Option(SimpleInterval)
 *  updated under And and Or operations and provides other required functionalities
 */
class FilterInterval(start: Long, end: Long) {

  if (start > Int.MaxValue.toLong || start <= 0 || end > Int.MaxValue.toLong || end <= 0) {
    throw new IllegalArgumentException
  }

  private var interval: Option[SimpleInterval] = try {
    Option(SimpleInterval("", start.toInt, end.toInt))
  } catch {
    case e: IllegalArgumentException => None
  }

  def actionAnd(other: FilterInterval): FilterInterval = {
    (interval, other.getSimpleInterval) match {
      case (Some(i), Some(j)) =>
        try {
          interval = Option(i.intersect(j))
        } catch {
          case e: IllegalArgumentException => interval = None
        }
      case _ => interval = None
    }
    this
  }

  def actionOr(other: FilterInterval): FilterInterval = {
    (interval, other.getSimpleInterval) match {
      case (Some(i), Some(j)) =>
        try {
          interval = Option(i.spanWith(j))
        } catch {
          case e: IllegalArgumentException => interval = None
        }
      case (Some(i), None) => interval = Option(i)
      case (None, Some(j)) => interval = Option(j)
      case (None, None) =>
    }
    this
  }

  def isDefined: Boolean = interval.isDefined

  def isEmpty: Boolean = interval.isEmpty

  def getSimpleInterval: Option[SimpleInterval] = interval

  def isSame(other: FilterInterval): Boolean = {
    (interval, other.getSimpleInterval) match {
      case (Some(i), Some(j)) =>
        i.getContig == j.getContig && i.getStart == j.getStart && i.getEnd == j.getEnd
      case (None, _) => other.isEmpty
      case (_, None) => interval.isEmpty
    }
  }

}

case class ParsedFilterResult(
    contig: FilterContig,
    startInterval: FilterInterval,
    endInterval: FilterInterval)

/** Contains filter parsing tools and other tools used to apply tabix index */
object TabixIndexHelper extends GlowLogging {

  /**
   * Parses filters provided by spark sql parser to generate the ParsedFilterResult=(contig,
   * startInterval, endInterval) that will be used to makeFilteredInterval
   * The parser acts recursively to handle And and Or logical operators. Therefore it is able to
   * handle any nested combination of And and Or operators.
   * Not operator is not supported and if used its effect will be passed to spark filter parser.
   *
   * @param filters The Seq of filters provided by spark sql
   * @return A ParsedFilterResult = (contig: FilterContig, startInterval: FilterInterval,
   *         endInterval: FilterInterval)
   *
   *         contig: Is a FilterContig that indicates the chromosome identified by the filter
   *         [Note: (in the current implementation, only one single chromosome must result from
   *         the combination of all filters for the tabix index to be used]
   *         contig.getContigName =
   *         None: if combination of all filters result in an inconsistent chromosome
   *         "": if combination of all filters does not specify a single chromosome or result in
   *         multiple chromosomes (tabix index use will be disabled by filteredVariantBlockRange)
   *
   *         startInterval: is a FilterInterval denoting variant "start" range that is indicated
   *         by combination of all filters. FilterInterval is 1-based but the 'start' column in
   *         filter is 0-based. The parser takes care of this conversion in all situations.
   *
   *         endInterval: is a FilterInterval denoting variant "end" range that is indicated by
   *         combination of all filters.
   */

  @VisibleForTesting
  private[vcf] def parseFilter(filters: Seq[Filter]): ParsedFilterResult = {

    // contig and startInterval and endInterval get updated as conditions are parsed.

    var paramsOK: Boolean = true

    val contig = new FilterContig("")
    val startInterval = new FilterInterval(1, Int.MaxValue)
    val endInterval = new FilterInterval(1, Int.MaxValue)

    for (x <- filters if paramsOK && contig.isDefined && startInterval.isDefined &&
      startInterval.isDefined) {
      x match {
        case And(left: Filter, right: Filter) =>
          // left and right are concatenated and parsed recursively
          val parsedLeftRight = parseFilter(Seq(left, right))

          // contig and start and end intervals are updated.
          contig.actionAnd(parsedLeftRight.contig)
          startInterval.actionAnd(parsedLeftRight.startInterval)
          endInterval.actionAnd(parsedLeftRight.endInterval)

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
            getSmallestQueryInterval(parsedLeft.startInterval, parsedLeft.endInterval).actionOr(
              getSmallestQueryInterval(parsedRight.startInterval, parsedRight.endInterval))

          contig.actionAnd(parsedLeft.contig.actionOr(parsedRight.contig))
          startInterval.actionAnd(orInterval)
          endInterval.actionAnd(orInterval)

        case EqualTo("contigName", value: String) =>
          contig.actionAnd(new FilterContig(value))

        // Note that in all the following cases, FilterInterval must be a 1-based interval
        // therefore the value in all 'start' cases is incremented by 1.
        case EqualTo(columnName: String, value: Long) =>
          if (columnName == "start") {
            if (value < 0 || value > Int.MaxValue) {
              paramsOK = false
            } else {
              startInterval.actionAnd(new FilterInterval(value + 1, value + 1))
            }
          } else if (columnName == "end") {
            if (value < 1 || value > Int.MaxValue) {
              paramsOK = false
            } else {
              endInterval.actionAnd(new FilterInterval(value, value))
            }
          }

        case GreaterThan(columnName: String, value: Long) =>
          if (columnName == "start") {
            if (value < -1 || value > Int.MaxValue) {
              paramsOK = false
            } else {
              startInterval.actionAnd(new FilterInterval(value + 2, Int.MaxValue))
            }
          } else if (columnName == "end") {
            if (value < 0 || value > Int.MaxValue) {
              paramsOK = false
            } else {
              endInterval.actionAnd(new FilterInterval(value + 1, Int.MaxValue))
            }
          }

        case GreaterThanOrEqual(columnName: String, value: Long) =>
          if (columnName == "start") {
            if (value < 0 || value > Int.MaxValue) {
              paramsOK = false
            } else {
              startInterval.actionAnd(new FilterInterval(value + 1, Int.MaxValue))
            }
          } else if (columnName == "end") {
            if (value < 1 || value > Int.MaxValue) {
              paramsOK = false
            } else {
              endInterval.actionAnd(new FilterInterval(value, Int.MaxValue))
            }
          }

        case LessThan(columnName: String, value: Long) =>
          if (columnName == "start") {
            if (value < 1 || value > Int.MaxValue) {
              paramsOK = false
            } else {
              startInterval.actionAnd(new FilterInterval(1, value))
            } // starts at 1 because FilterInterval must be 1-based interval.
          } else if (columnName == "end") {
            if (value < 2 || value > Int.MaxValue) {
              paramsOK = false
            } else {
              endInterval.actionAnd(new FilterInterval(1, value - 1))
            }
          }

        case LessThanOrEqual(columnName: String, value: Long) =>
          if (columnName == "start") {
            if (value < 0 || value > Int.MaxValue) {
              paramsOK = false
            } else {
              startInterval.actionAnd(new FilterInterval(1, value + 1))
            } // starts at 1 because FilterInterval must be 1-based interval.
          } else if (columnName == "end") {
            if (value < 1 || value > Int.MaxValue) {
              paramsOK = false
            } else {
              endInterval.actionAnd(new FilterInterval(1, value))
            }
          }

        case _ =>
      }
    }

    if (!paramsOK) {
      ParsedFilterResult(
        new FilterContig(""),
        new FilterInterval(1, Int.MaxValue),
        new FilterInterval(1, Int.MaxValue))
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
      startInterval: FilterInterval,
      endInterval: FilterInterval): FilterInterval = {

    (startInterval.getSimpleInterval, endInterval.getSimpleInterval) match {
      case (Some(si), Some(ei)) =>
        var smallestQueryInterval: SimpleInterval = null

        if (si.getStart > ei.getEnd) {
          new FilterInterval(2, 1)
        } else {
          if (si.overlaps(ei)) {
            smallestQueryInterval = si.intersect(ei)
          } else {
            smallestQueryInterval = SimpleInterval("", ei.getStart, ei.getStart)
          }
          new FilterInterval(smallestQueryInterval.getStart, smallestQueryInterval.getEnd)
        }
      case _ => new FilterInterval(2, 1)
    }
  }

  /** Getting Seq of filters, returns an Option[SimpleInterval] representing contig and SQI,
   * and takes care of useFilterParser option.
   * @param filters
   * @return Option[SimpleInterval] indicating contig and SQI of the filtered interval;
   *        None: if filter results in a null set of records;
   *        Contig is empty if filtering is skipped due to multiple contigs or unsupported elements.
   */
  def makeFilteredInterval(
      filters: Seq[Filter],
      useFilterParser: Boolean,
      useIndex: Boolean): Option[SimpleInterval] = {

    if (useFilterParser) {
      val parsedFilterResult = parseFilter(filters)

      (
        parsedFilterResult.contig.getContigName,
        getSmallestQueryInterval(
          parsedFilterResult.startInterval,
          parsedFilterResult.endInterval
        ).getSimpleInterval
      ) match {
        case (Some(c), Some(i)) => Option(SimpleInterval(c, i.getStart, i.getEnd))
        case _ => None
      }
    } else {
      if (useIndex) {
        logger.info("Error: Filter parser is deactivated while requesting index use.")
        throw new IllegalArgumentException
      }
      Option(SimpleInterval("", 1, Int.MaxValue))
    }
  }

  /**
   * Performs all the checks needed to use tabix index
   * If a check fails generates appropriate message
   * Otherwise generates block range by querying tabix index based
   * on the filteredSimpleInterval produces by makeFilteredInterval.
   */
  def getFileRangeToRead(
      hadoopFs: FileSystem,
      file: PartitionedFile,
      conf: Configuration,
      hasFilter: Boolean, // true if user did specify a filter
      useIndex: Boolean,
      filteredSimpleInterval: Option[SimpleInterval] // see description for makeFilteredInterval
      // for what filteredSimpleInterval is
  ): Option[(Long, Long)] = {

    // Do not read index files
    if (file.filePath.endsWith(VCFFileFormat.INDEX_SUFFIX)) {
      return None
    }

    val path = new Path(file.filePath)
    val indexFile = new Path(file.filePath + VCFFileFormat.INDEX_SUFFIX)
    val isGzip = VCFFileFormat.isGzip(file, conf)

    filteredSimpleInterval match {
      case Some(interval) =>
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
        } else if (interval.getContig.isEmpty) {
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
            .getBlocks(interval.getContig, interval.getStart, interval.getEnd)
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

      case None =>
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

}
