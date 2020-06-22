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

import com.google.gson.Gson

import io.projectglow.common.GlowLogging

/**
 * These are trait/objects/case classes to log hls events.
 */
trait HlsUsageLogging extends GlowLogging {

  protected def recordHlsUsage(
      metric: MetricDefinition,
      tags: Map[TagDefinition, String] = Map.empty,
      blob: String = null): Unit = {

    logger.info(
      s"${metric.name}:[${{
        if (blob == null) {
          tags.values
        } else {
          tags.values ++ Iterable(blob)
        }
      }.mkString(",")}]"
    )
  }

  protected def hlsJsonBuilder(options: Map[String, Any]): String = {
    { new Gson }.toJson(options.asJava)
  }

}

case class MetricDefinition(name: String, description: String)

case class TagDefinition(name: String, description: String)

object HlsMetricDefinitions {
  val EVENT_HLS_USAGE = MetricDefinition(
    "hlsUsage",
    description = "Umbrella event for event tracking of HLS services"
  )
}

object HlsTagDefinitions {
  val TAG_EVENT_TYPE = TagDefinition(
    name = "eventType",
    description = "The type of event that occurred."
  )
}

object HlsTagValues {
  val EVENT_PIPE = "pipe"

  val EVENT_NORMALIZE_VARIANTS = "normalizeVariants"

  val EVENT_VCF_READ = "vcfRead"

  val EVENT_VCF_WRITE = "vcfWrite"

  val EVENT_BIGVCF_WRITE = "bigVcfWrite"

  val EVENT_BGEN_READ = "bgenRead"

  val EVENT_BGEN_WRITE = "bgenWrite"

  val EVENT_PLINK_READ = "plinkRead"

  val EVENT_WGR_RIDGE_REDUCE_FIT = "wgrRidgeReduceFit"

  val EVENT_WGR_RIDGE_REDUCE_TRANSFORM = "wgrRidgeReduceTransform"

  val EVENT_WGR_RIDGE_REGRESSION_FIT = "wgrRidgeRegressionFit"

  val EVENT_WGR_RIDGE_REGRESSION_TRANSFORM = "wgrRidgeRegressionTransform"
}
