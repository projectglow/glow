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

package io.projectglow.common.logging

import scala.collection.JavaConverters._

trait HlsEventRecorder extends HlsUsageLogging {

  // Wrapper to simplify recording an HLS usage event
  def recordHlsEvent(tag: String, options: Map[String, Any] = Map.empty): Unit = {
    val metric = HlsMetricDefinitions.EVENT_HLS_USAGE
    val tags = Map(HlsTagDefinitions.TAG_EVENT_TYPE -> tag)

    if (options.isEmpty) {
      recordHlsUsage(metric, tags)
    } else {
      recordHlsUsage(metric, tags, blob = hlsJsonBuilder(options))
    }
  }

}

object PythonHlsEventRecorder extends HlsEventRecorder {
  def recordHlsEvent(tag: String, options: java.util.Map[String, Any]): Unit = {
    super.recordHlsEvent(tag, options.asScala.toMap)
  }
}
