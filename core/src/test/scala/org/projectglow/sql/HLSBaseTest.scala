package org.projectglow.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.{DebugFilesystem, SparkConf}
import org.scalatest.concurrent.{AbstractPatienceConfiguration, Eventually}
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{FunSuite, Tag}

import org.projectglow.common.{HLSLogging, TestUtils}
import org.projectglow.core.common.{HLSLogging, TestUtils}

abstract class HLSBaseTest
    extends FunSuite
    with SharedSparkSession
    with HLSLogging
    with HLSTestData
    with TestUtils
    with JenkinsTestPatience {

  override protected def sparkConf: SparkConf = {
    super
      .sparkConf
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

  override def afterEach(): Unit = {
    DebugFilesystem.clearOpenStreams()
    super.afterEach()
  }
}

/**
 * Unit-test patience config to use with unit tests that use scala test's eventually and other
 * asynchronous checks. This will override the default timeout and check interval so they are
 * more likely to pass in highly loaded CI environments.
 *
 */
trait JenkinsTestPatience extends AbstractPatienceConfiguration with Eventually {

  /**
   * The total timeout to wait for `eventually` blocks to succeed
   */
  final override implicit val patienceConfig: PatienceConfig =
    if (sys.env.get("JENKINS_HOST").nonEmpty) {
      // increase the timeout on jenkins where parallelizing causes things to be very slow
      PatienceConfig(Span(10, Seconds), Span(50, Milliseconds))
    } else {
      // use the default timeout on local machines so failures don't hang for a long time
      PatienceConfig(Span(5, Seconds), Span(15, Milliseconds))
    }
}
