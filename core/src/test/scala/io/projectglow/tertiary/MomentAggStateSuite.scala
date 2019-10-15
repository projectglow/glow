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

package io.projectglow.tertiary

import io.projectglow.sql.GlowBaseTest
import io.projectglow.sql.expressions.MomentAggState
import io.projectglow.sql.GlowBaseTest
import io.projectglow.sql.expressions.MomentAggState

class MomentAggStateSuite extends GlowBaseTest {
  test("merge") {
    val s1 = MomentAggState(5, 0, 10, 2, 1)
    val s2 = MomentAggState(3, 1, 11, 1, 2)
    val merged = MomentAggState.merge(s1, s2)
    val expected = MomentAggState(8, 0, 11, 1.625, 1 + 2 + -1 * -0.125 * 5 * 3)
    assert(merged == expected)
  }

  test("merge (count == 0)") {
    val s1 = MomentAggState()
    val s2 = MomentAggState()
    val merged = MomentAggState.merge(s1, s2)
    val expected = MomentAggState()
    assert(merged == expected)
  }

  test("merge (left count == 0)") {
    val s1 = MomentAggState(0, 0, 10, 2, 1)
    val s2 = MomentAggState(1, 1, 11, 1, 2)
    val merged = MomentAggState.merge(s1, s2)
    val expected = s2
    assert(merged == expected)
  }

  test("merge (right count == 0)") {
    val s1 = MomentAggState(1, 0, 10, 2, 1)
    val s2 = MomentAggState(0, 1, 11, 1, 2)
    val merged = MomentAggState.merge(s1, s2)
    val expected = s1
    assert(merged == expected)
  }

  test("update") {
    val state = MomentAggState(1, 1, 1, 1, 0)
    state.update(10)
    val expected = MomentAggState(2, 1, 10, 5.5, 40.5)
    assert(state == expected)
  }
}
