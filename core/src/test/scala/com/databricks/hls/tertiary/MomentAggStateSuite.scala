package com.databricks.hls.tertiary

import com.databricks.hls.sql.HLSBaseTest

class MomentAggStateSuite extends HLSBaseTest {
  test("merge") {
    val s1 = MomentAggState(5, 0, 10, 2, 1)
    val s2 = MomentAggState(3, 1, 11, 1, 2)
    val merged = MomentAggState.merge(s1, s2)
    val expected = MomentAggState(8, 0, 11, 1.625, 1 + 2 + -1 * -0.125 * 5 * 3)
  }
}
