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

import scala.math.{max, min}

object GenomicIntervalUtils {

  /**
   * A class representing a closed interval of longs that also accommodates an empty interval.
   */
  class LongInterval() extends Serializable {
    var interval: Option[(Long, Long)] = None

    def this(start: Long, end: Long) {
      this()
      if (end >= start) {
        this.interval = Option((start, end))
      }
    }

    def start: Long =
      if (nonEmpty) {
        interval.get._1
      } else {
        throw new NoSuchElementException("Interval is empty!")
      }

    def end: Long =
      if (nonEmpty) {
        interval.get._2
      } else {
        throw new NoSuchElementException("Interval is empty!")
      }

    def isEmpty: Boolean = interval.isEmpty

    def nonEmpty: Boolean = interval.nonEmpty

    def isSame(that: LongInterval): Boolean = {
      (nonEmpty, that.nonEmpty) match {
        case (true, true) =>
          start == that.start && end == that.end
        case (false, false) => true
        case _ => false
      }
    }

    def overlaps(that: LongInterval): Boolean = {
      if (nonEmpty && that.nonEmpty) {
        start <= that.end && that.start <= end
      } else {
        false
      }
    }

    def intersect(that: LongInterval): LongInterval = {
      (nonEmpty, that.nonEmpty) match {
        case (true, true) =>
          new LongInterval(max(start, that.start), min(end, that.end))
        case _ => new LongInterval()
      }
    }

    /**
     * Gives the smallest interval that contains this and that
     */
    def spanWith(that: LongInterval): LongInterval = {
      (nonEmpty, that.nonEmpty) match {
        case (true, true) =>
          new LongInterval(math.min(start, that.start), math.max(end, that.end))
        case (true, false) => this
        case (false, true) => that
        case (false, false) => new LongInterval()
      }
    }
  }

  /**
   * A class representing a single contig that also accommodates an empty contig and "any contig".
   * Any set of contigs that are not singleton or empty is called "any contig". Contig has a name
   * only if it is a single contig.
   */
  class Contig(val isAnyContig: Boolean = false) extends Serializable {
    val EMPTY = 0
    val SINGLE_CONTIG = 1
    val ANY_CONTIG = 2

    var status = if (isAnyContig) ANY_CONTIG else EMPTY
    var contig: Option[String] = None

    def this(contig: String) {
      this()
      this.status = SINGLE_CONTIG
      this.contig = Option(contig)
    }

    def isEmpty: Boolean = status == EMPTY

    def nonEmpty: Boolean = !isEmpty

    def isSingleContig: Boolean = status == SINGLE_CONTIG

    def name: String =
      if (isSingleContig) {
        contig.get
      } else {
        throw new NoSuchElementException("Contig is empty or any contig!")
      }

    def isSame(that: Contig): Boolean = {
      (status, that.status) match {
        case (SINGLE_CONTIG, SINGLE_CONTIG) => name == that.name
        case (ANY_CONTIG, ANY_CONTIG) | (EMPTY, EMPTY) => true
        case _ => false
      }
    }

    def intersect(that: Contig): Contig = {
      (status, that.status) match {
        case (SINGLE_CONTIG, SINGLE_CONTIG) if (name == that.name) => this
        case (SINGLE_CONTIG, ANY_CONTIG) => this
        case (ANY_CONTIG, SINGLE_CONTIG) => that
        case (ANY_CONTIG, ANY_CONTIG) => this
        case _ =>
          new Contig()
      }
    }

    def union(that: Contig): Contig = {
      (status, that.status) match {
        case (SINGLE_CONTIG, SINGLE_CONTIG) if (name == that.name) => this
        case (SINGLE_CONTIG, EMPTY) => this
        case (EMPTY, SINGLE_CONTIG) => that
        case (EMPTY, EMPTY) => this
        case _ =>
          new Contig(true)
      }
    }

  }
}
