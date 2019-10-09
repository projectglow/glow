package org.projectglow.sql

import java.nio.file.Paths

trait GlowTestData {
  final lazy val testDataHome = Paths
    .get(
      sys.props.getOrElse("test.dir", ""),
      "test-data"
    )
    .toString
}
