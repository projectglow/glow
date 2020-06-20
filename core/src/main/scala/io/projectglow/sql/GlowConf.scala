package io.projectglow.sql

import org.apache.spark.sql.internal.SQLConf

object GlowConf {
  val FAST_VCF_READER_ENABLED =
    SQLConf.buildConf("io.projectglow.vcf.fastReaderEnabled")
      .doc("Use fast VCF reader")
      .booleanConf
      .createWithDefault(false)
}
