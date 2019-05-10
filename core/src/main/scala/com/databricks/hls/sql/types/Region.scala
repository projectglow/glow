/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.hls.sql.types

import org.bdgenomics.utils.interval.array.Interval
import scala.math.{max, min}

/**
 * Represents a contiguous region of the reference genome.
 * Defines a half open interval [start, end)
 *
 * @param start The 0-based residue-coordinate for the start of the region
 * @param end The 0-based residue-coordinate for the first residue <i>after</i> the start
 *            which is <i>not</i> in the region -- i.e. [start, end) define a 0-based
 *            half-open interval.
 */
case class Region(start: Long, end: Long) extends Interval[Region] {

  override def overlaps(other: Region): Boolean = covers(other)

  override def covers(other: Region): Boolean = start < other.end && end > other.start

  override def compareTo(other: Region): Int = {
    if (start != other.start) {
      start.compareTo(other.start)
    } else {
      end.compareTo(other.end)
    }
  }

  override def distance(other: Region): Option[Long] = {
    if (overlaps(other)) {
      Some(0)
    } else {
      Some(max(start, other.start) - min(end, other.end))
    }
  }
}
