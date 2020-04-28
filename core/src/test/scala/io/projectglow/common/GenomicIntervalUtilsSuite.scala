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

import java.util.NoSuchElementException

import io.projectglow.common.GenomicIntervalUtils.{Contig, LongInterval}
import io.projectglow.sql.GlowBaseTest

class GenomicIntervalUtilsSuite extends GlowBaseTest {
  val i = new LongInterval(1, 10)
  val k = new LongInterval(5, 20)
  val l = new LongInterval(25, 45)
  val e1 = new LongInterval()
  val e2 = new LongInterval(10, 1)

  test("LongInterval: start and end") {
    assert(i.start == 1)
    assert(i.end == 10)
    assertThrows[NoSuchElementException](e1.start)
    assertThrows[NoSuchElementException](e1.end)
    assertThrows[NoSuchElementException](e2.start)
    assertThrows[NoSuchElementException](e2.end)
  }

  test("LongInterval: isEmpty and nonEmpty") {
    assert(!i.isEmpty)
    assert(i.nonEmpty)
    assert(e1.isEmpty)
    assert(e2.isEmpty)
  }

  test("LongInterval: isSame") {
    assert(i.isSame(i))
    assert(e1.isSame(e2))
    assert(!i.isSame(k))
  }

  test("LongInterval: overlaps") {
    assert(i.overlaps(k))
    assert(!i.overlaps(l))
    assert(!i.overlaps(e1))
    assert(!e1.overlaps(e2))
  }

  test("LongInterval: intersect") {
    assert(i.intersect(i).isSame(i))
    assert(i.intersect(k).isSame(new LongInterval(5, 10)))
    assert(i.intersect(l).isSame(e1))
    assert(e1.intersect(i).isSame(e1))
    assert(e1.intersect(e2).isSame(e1))
  }

  test("LongInterval: spanWith") {
    assert(i.spanWith(i).isSame(i))
    assert(i.spanWith(k).isSame(new LongInterval(1, 20)))
    assert(i.spanWith(l).isSame(new LongInterval(1, 45)))
    assert(e1.spanWith(i).isSame(i))
    assert(e1.spanWith(e2).isSame(e1))
  }

  val sC = new Contig("1")
  val sD = new Contig("2")
  val eC = new Contig()
  val aC = new Contig(true)

  test("Contig: isEmpty, nonEmpty, isSingleContig, isAnyContig") {
    assert(!sC.isEmpty && eC.isEmpty && !aC.isEmpty)
    assert(sC.nonEmpty && !eC.nonEmpty && aC.nonEmpty)
    assert(sC.isSingleContig && !eC.isSingleContig && !aC.isSingleContig)
    assert(!sC.isAnyContig && !eC.isAnyContig && aC.isAnyContig)
  }

  test("Contig: name") {
    assert(sC.name == "1")
    assertThrows[NoSuchElementException](eC.name)
    assertThrows[NoSuchElementException](aC.name)
  }

  test("Contig: isSame") {
    assert(sC.isSame(sC))
    assert(!sC.isSame(sD))
    assert(eC.isSame(eC))
    assert(aC.isSame(aC))
    assert(!aC.isSame(sC))
    assert(!aC.isSame(eC))
    assert(!eC.isSame(sC))
  }

  test("Contig: intersect ") {
    assert(sC.intersect(sC).isSame(sC))
    assert(sC.intersect(sD).isSame(eC))
    assert(eC.intersect(eC).isSame(eC))
    assert(aC.intersect(aC).isSame(aC))
    assert(aC.intersect(sC).isSame(sC))
    assert(aC.intersect(eC).isSame(eC))
    assert(eC.intersect(sC).isSame(eC))
  }

  test("Contig: union ") {
    assert(sC.union(sC).isSame(sC))
    assert(sC.union(sD).isSame(aC))
    assert(eC.union(eC).isSame(eC))
    assert(aC.union(aC).isSame(aC))
    assert(aC.union(sC).isSame(aC))
    assert(aC.union(eC).isSame(aC))
    assert(eC.union(sC).isSame(sC))
  }

}
