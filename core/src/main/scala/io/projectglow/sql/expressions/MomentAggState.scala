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

package io.projectglow.sql.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import io.projectglow.common.GlowLogging

/**
 * The state necessary for maintaining moment based aggregations, currently only supported up to m2.
 *
 * This functionality is based on the
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.CentralMomentAgg]] implementation in Spark
 * and is used to compute summary statistics on arrays as well across many rows for sample
 * based aggregations.
 */
case class MomentAggState(
    var count: Long = 0,
    var min: Double = 0,
    var max: Double = 0,
    var mean: Double = 0,
    var m2: Double = 0) {

  def this() = {
    this(0, 0, 0, 0, 0)
  }

  def update(element: Double): Unit = {
    count += 1
    val delta = element - mean
    val deltaN = delta / count
    mean += deltaN
    m2 += delta * (delta - deltaN)
    if (element < min || count == 1) {
      min = element
    }

    if (element > max || count == 1) {
      max = element
    }
  }

  def update(element: Long): Unit = update(element.toDouble)
  def update(element: Int): Unit = update(element.toDouble)
  def update(element: Float): Unit = update(element.toDouble)

  def toInternalRow(row: InternalRow): InternalRow = {
    row.update(0, if (count > 0) mean else null)
    row.update(1, if (count > 0) Math.sqrt(m2 / (count - 1)))
    row.update(2, if (count > 0) min else null)
    row.update(3, if (count > 0) max else null)
    row
  }

  def toInternalRow: InternalRow = {
    toInternalRow(new GenericInternalRow(4))
  }
}

object MomentAggState extends GlowLogging {
  val schema = StructType(
    Seq(
      StructField("mean", DoubleType),
      StructField("stdDev", DoubleType),
      StructField("min", DoubleType),
      StructField("max", DoubleType)
    )
  )

  def merge(s1: MomentAggState, s2: MomentAggState): MomentAggState = {
    if (s1.count == 0) {
      return s2
    } else if (s2.count == 0) {
      return s1
    }

    val newState = MomentAggState()
    newState.count = s1.count + s2.count
    val delta = s2.mean - s1.mean
    val deltaN = delta / newState.count
    newState.mean = s1.mean + deltaN * s2.count

    // higher order moments computed according to:
    // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
    newState.m2 = s1.m2 + s2.m2 + delta * deltaN * s1.count * s2.count

    newState.min = Math.min(s1.min, s2.min)
    newState.max = Math.max(s1.max, s2.max)
    newState
  }
}
