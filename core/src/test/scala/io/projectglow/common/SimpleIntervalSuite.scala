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

import io.projectglow.sql.GlowBaseTest

class SimpleIntervalSuite extends GlowBaseTest {
  lazy val i1 = SimpleInterval("C", 1, 10)
  lazy val i2 = SimpleInterval("D", 1, 10)
  lazy val i3 = SimpleInterval("C", 5, 20)
  lazy val i4 = SimpleInterval("C", 15, 40)
  lazy val i5 = SimpleInterval("C", 1, 10)

  test("Illegal argument") {
    assertThrows[IllegalArgumentException](SimpleInterval("C", 11, 10))
    assertThrows[IllegalArgumentException](SimpleInterval("C", 0, 10))
  }

  test("getContig, getStart, getEnd") {
    assert(i1.getContig == "C")
    assert(i1.getStart == 1)
    assert(i1.getEnd == 10)
    assert(i1 == SimpleInterval("C", 1, 10))
  }

  test("overlaps") {
    assert(!i1.overlaps(i2))
    assert(i1.overlaps(i1))
    assert(i1.overlaps(i3))
    assert(!i1.overlaps(i4))
  }

  test("intersect") {
    assert(i1.intersect(i1) == i1)
    assert(i1.intersect(i3) == SimpleInterval("C", 5, 10))
    assertThrows[IllegalArgumentException](i1.intersect(i2))
    assertThrows[IllegalArgumentException](i1.intersect(i4))
  }

  test("spanWith") {
    assert(i1.spanWith(i1) == i1)
    assert(i1.spanWith(i3) == SimpleInterval("C", 1, 20))
    assert(i1.spanWith(i4) == SimpleInterval("C", 1, 40))
    assertThrows[IllegalArgumentException](i1.spanWith(i2))
  }
}
