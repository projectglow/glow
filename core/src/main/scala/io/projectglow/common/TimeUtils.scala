/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.projectglow.common

import scala.concurrent.duration._

object TimeUtils {
  def durationString(duration: FiniteDuration): String = {
    val hours = duration.toHours
    val minutes = (duration - hours.hours).toMinutes
    val seconds = (duration - hours.hours - minutes.minutes).toSeconds
    s"${hours}h${minutes}m${seconds}s"
  }
}
