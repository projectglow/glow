package com.databricks.hls.common.logging

import com.databricks.hls.common.HLSLogging

/**
 * These are objects/case classes/traits to log HLS events.
 */
case class MetricDefinition(name: String, description: String)
case class TagDefinition(name: String, description: String)

object HlsMetricDefinitions {
  val EVENT_HLS_USAGE = MetricDefinition(
    "hlsUsageEvent",
    description = "Umbrella event for event tracking of HLS services"
  )
}

object HlsTagDefinitions {
  val TAG_EVENT_TYPE = TagDefinition(
    name = "eventType",
    description = "The type of event that occurred."
  )

  val TAG_HLS_PIPE_CMD = TagDefinition(
    name = "pipeCmd",
    description = "Tool used in HLS pipe command"
  )

  val TAG_HLS_NORMALIZE_VARIANTS_MODE = TagDefinition(
    name = "variantNormalizerMode",
    description = "Mode of HLS variant normalizer"
  )

  val TAG_HLS_VCF_READ_OPTIONS = TagDefinition(
    name = "vcfReadOptions",
    description = "Options passed to HLS VCF reader"
  )

  val TAG_HLS_VCF_WRITE_COMPRESSION = TagDefinition(
    name = "vcfWriteCompression",
    description = "Compression codec used by HLS VCF writer"
  )
}

object HlsTagValues {
  val EVENT_PIPE = "pipe"
  val PIPE_CMD_SAIGE = "saige"
  val PIPE_CMD_PLINK = "plink"

  val EVENT_NORMALIZE_VARIANTS = "normalizeVariants"

  val EVENT_VCF_READ = "vcfRead"
  val VCF_READ_DONT_USE_TABIX_INDEX = "dontUseTabixIndex"
  val VCF_READ_DONT_USE_FILTER_PARSER = "dontUSeFilterParser"

  val EVENT_VCF_WRITE = "vcfWrite"

  val EVENT_BGEN_READ = "bgenRead"

  val EVENT_BGEN_WRITE = "bgenWrite"
}

trait HLSUsageLogging extends HLSLogging {
  protected def recordHlsUsage(
      metric: MetricDefinition,
      tags: Map[TagDefinition, String] = Map.empty): Unit = {
    logger.info({ s"${metric.name}:[${tags.values.reduceLeft(_ + "," + _)}" } + "]")
  }
}
