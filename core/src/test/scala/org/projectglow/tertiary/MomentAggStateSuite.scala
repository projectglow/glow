package org.projectglow.tertiary

import org.projectglow.core.sql.HLSBaseTest
import org.projectglow.core.sql.expressions.MomentAggState
import org.projectglow.sql.HLSBaseTest
import org.projectglow.sql.expressions.MomentAggState

class MomentAggStateSuite extends HLSBaseTest {
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
