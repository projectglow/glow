/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.spark.sql.hls.dsl
import org.apache.spark.sql.functions._

import com.databricks.hls.sql.expressions._
import org.apache.spark.sql.Column

object expressions { // scalastyle:ignore

  def overlaps(x1: Column, x2: Column, y1: Column, y2: Column): Column = {
    x1 < y2 && x2 > y1
  }

  def bin(x: Column, y: Column, binSize: Int): Column =
    Column(BinIdGenerator(x.expr, y.expr, binSize))

  def transform(x: Column, transformer: Function1[Char, Char]): Column =
    Column(StringTransformExpression(x.expr, transformer))

  def ascii_char_split(x: Column, split: Char): Column =
    Column(AsciiCharSplit(x.expr, lit(split).expr))
}
