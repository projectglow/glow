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

package io.projectglow.common

import htsjdk.samtools.util.Locatable

/**
 * Minimal immutable class representing a 1-based closed ended genomic interval
 * Simplified version of org.broadinstitute.hellbender.utils.SimpleInterval
 */
case class SimpleInterval(contig: String, start: Int, end: Int)
    extends Locatable
    with Serializable {

  if (!isValid) {
    throw new IllegalArgumentException(
      s"Invalid interval. Contig: $contig, start: $start, end: $end")
  }

  def isValid: Boolean = start > 0 && end >= start

  override def getContig: String = contig

  override def getStart: Int = start

  override def getEnd: Int = end

  def overlaps(that: SimpleInterval): Boolean = {
    this.contig == that.getContig && this.start <= that.getEnd && that.getStart <= this.end
  }

  def intersect(that: SimpleInterval): SimpleInterval = {
    if (!overlaps(that)) {
      throw new IllegalArgumentException("The two intervals need to overlap")
    }
    new SimpleInterval(getContig, math.max(getStart, that.getStart), math.min(getEnd, that.getEnd))
  }

  def spanWith(that: SimpleInterval): SimpleInterval = {
    if (getContig != that.getContig) {
      throw new IllegalArgumentException("Cannot get span for intervals on different contigs")
    }
    new SimpleInterval(contig, math.min(start, that.getStart), math.max(end, that.getEnd))
  }
}
