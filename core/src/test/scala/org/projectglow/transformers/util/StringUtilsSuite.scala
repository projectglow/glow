package org.projectglow.transformers.util

import org.scalatest.FunSuite

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
