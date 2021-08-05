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

package io.projectglow.transformers.util

import io.projectglow.SparkTestShim.FunSuite

class StringUtilsSuite extends FunSuite {
  private def testSnakeConversion(name: String, input: String, expected: String): Unit = {
    test(name) {
      assert(StringUtils.toSnakeCase(input) == expected)
    }
  }

  testSnakeConversion(
    "doesn't change lower case string",
    "monkey",
    "monkey"
  )

  testSnakeConversion(
    "doesn't change lower case string with underscores",
    "mon_key",
    "mon_key"
  )

  testSnakeConversion(
    "simple camel to snake case",
    "monKey",
    "mon_key"
  )

  testSnakeConversion(
    "upper camel to snake",
    "MonKey",
    "mon_key"
  )

  testSnakeConversion(
    "mixed",
    "MonKe_y",
    "mon_ke_y"
  )

  test("SnakeCaseMap") {
    val m = new SnakeCaseMap(
      Map(
        "AniMal" -> "MonKey"
      ))
    assert(m("AniMal") == "MonKey")
    assert(m("aniMal") == "MonKey")
    assert(m("ani_mal") == "MonKey")
    assert(!m.contains("animal"))
  }

  test("SnakeCaseMap (add / subtract)") {
    val base = new SnakeCaseMap(
      Map(
        "AniMal" -> "MonKey",
        "vegeTable" -> "carrot"
      ))
    val added = base + ("kEy" -> "value")
    assert(added("ani_mal") == "MonKey")
    assert(added("k_ey") == "value")

    val subtracted = base - "vege_table"
    assert(subtracted("ani_mal") == "MonKey")
    assert(subtracted.size == 1)
  }
}
