/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.hls.sql.types

import com.databricks.hls.sql.HLSBaseTest

class RegionSuite extends HLSBaseTest {

  test("overlaps") {
    val testRegion = Region(1, 10)
    val candidates = Seq(
      (true, Region(0, 3)),
      (true, Region(2, 5)),
      (true, Region(5, 11)),
      (false, Region(11, 30)),
      (true, Region(0, 15))
    )

    candidates.foreach {
      case (expected, candidate) =>
        assert(expected == testRegion.overlaps(candidate))
    }
  }

  test("distance") {
    assert(Region(1, 3).distance(Region(2, 5)) contains 0)
    assert(Region(1, 3).distance(Region(4, 5)) contains 1)
    assert(Region(1, 4).distance(Region(2, 3)) contains 0)
  }

  test("compare to") {
    assert(Region(1, 3).compareTo(Region(2, 4)) === -1)
    assert(Region(1, 3).compareTo(Region(1, 4)) === -1)
    assert(Region(1, 3).compareTo(Region(1, 3)) === 0)
    assert(Region(1, 3).compareTo(Region(1, 2)) === 1)
    assert(Region(1, 3).compareTo(Region(4, 5)) === -1)
    assert(Region(4, 5).compareTo(Region(1, 2)) === 1)
  }
}
