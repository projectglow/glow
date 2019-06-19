package com.databricks.hls.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.{FunSuite, Tag}

import com.databricks.hls.common.HLSLogging

abstract class HLSBaseTest
    extends FunSuite
    with SharedSparkSession
    with HLSLogging
    with HLSTestData {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.maxResultSize", "0")
      .set("spark.kryo.registrator", "org.broadinstitute.hellbender.engine.spark.GATKRegistrator")
      .set("spark.kryoserializer.buffer.max", "2047m")
      .set("spark.kryo.registrationRequired", "false")
      .set(
        "spark.hadoop.io.compression.codecs",
        "org.seqdoop.hadoop_bam.util.BGZFCodec,org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec"
      )
  }

  override protected def createSparkSession = {
    val session = super.createSparkSession
    SqlExtensionProvider.register(session)
    SparkSession.setActiveSession(session)
    session
  }

  protected def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
      testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" ($param)", testTags: _*)(testFun(param))
    }
  }
}
