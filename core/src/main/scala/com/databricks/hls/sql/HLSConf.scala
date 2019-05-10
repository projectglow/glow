/**
 * Copyright (C) 2018 Databricks, Inc.
 *
 * Portions of this software incorporate or are derived from software contained within Apache Spark,
 * and this modified software differs from the Apache Spark software provided under the Apache
 * License, Version 2.0, a copy of which you may obtain at
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package com.databricks.hls.sql

import org.apache.spark.sql.internal.SQLConf.buildConf

/**
 * Configuration for HLS Specific SQL Features
 */
object HLSConf {
  val ASSEMBLY_REGION_BIN_SIZE =
    buildConf("spark.databricks.hls.pipeline.dnaseq.assemblyRegionBinSize")
      .internal()
      .doc("The Bin Size to choose for Partitioning Assembly Reads")
      .intConf
      .createWithDefault(5000)

  val ASSEMBLY_REGION_NUM_PARTITIONS =
    buildConf("spark.databricks.hls.pipeline.dnaseq.assemblyRegionNumPartitions")
      .internal()
      .doc("The number of partitions for Assembly Reads")
      .intConf
      .createWithDefault(1024)

  val JOINT_GENOTYPING_BIN_SIZE =
    buildConf("spark.databricks.hls.pipeline.joint.jointGenotypingBinSize")
      .internal()
      .doc("The Bin Size to choose for Partitioning Variant Contexts in Joint Genotyping")
      .intConf
      .createWithDefault(500000)

  val JOINT_GENOTYPING_NUM_BIN_PARTITIONS =
    buildConf("spark.databricks.hls.pipeline.joint.jointGenotypingNumBinPartitions")
      .internal()
      .doc("The number of partitions for binning Variant Contexts in Joint Genotyping")
      .intConf
      .createWithDefault(6000)

  val JOINT_GENOTYPING_NUM_SHUFFLE_PARTITIONS =
    buildConf("spark.databricks.hls.pipeline.joint.jointGenotypingNumPartitions")
      .internal()
      .doc("The number of partitions for shuffles in Joint Genotyping")
      .intConf
      .createOptional

  val JOINT_GENOTYPING_RANGE_JOIN_BIN_SIZE =
    buildConf("spark.databricks.hls.pipeline.joint.jointGenotypingRangeJoinBinSize")
      .internal()
      .doc(
        "The Bin Size to choose for Range Join on Variant Loci and Variant Contexts " +
        "in Joint Genotyping"
      )
      .intConf
      .createWithDefault(1000)

  val PIPELINE_NUM_CONCURRENT_SAMPLES =
    buildConf("spark.databricks.hls.pipeline.dnaseq.numConcurrentSamples")
      .internal()
      .doc("The Number of concurrent samples to process")
      .intConf
      .createWithDefault(1) // for exomes raise this number to process more samples in parallel

  val PIPELINE_TIMEOUT = buildConf("spark.databricks.hls.pipeline.dnaseq.timeout")
    .internal()
    .doc("The Timeout in hours for each run of the Pipeline")
    .intConf
    .createWithDefault(12)

  val PIPELINE_SKIP_FAILED_SAMPLES =
    buildConf("spark.databricks.hls.pipeline.dnaseq.skipFailedSamples")
      .internal()
      .doc("Continue processing even if some samples fail")
      .booleanConf
      .createWithDefault(false)

  val VARIANT_INGEST_BATCH_SIZE =
    buildConf("spark.databricks.hls.pipeline.joint.variantIngestBatchSize")
      .internal()
      .doc("Number of gVCFs to read as a batch during variant ingest")
      .intConf
      .createWithDefault(500)

  val VARIANT_INGEST_NUM_PARTITIONS =
    buildConf("spark.databricks.hls.pipeline.joint.variantIngestNumPartitions")
      .internal()
      .doc("Number of partitions for coalescing gVCF datasets during variant ingest")
      .intConf
      .createOptional

  val VARIANT_INGEST_SKIP_STATS_COLLECTION =
    buildConf("spark.databricks.hls.pipeline.joint.variantIngestSkipStatsCollection")
      .internal()
      .doc("Whether to skip stats collection for Delta data skipping")
      .booleanConf
      .createWithDefault(true)
}
