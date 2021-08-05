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

package io.projectglow.sql

import htsjdk.samtools.util.Log
import org.apache.spark.sql.SparkSession
import org.apache.spark.DebugFilesystem
import org.scalatest.concurrent.{AbstractPatienceConfiguration, Eventually}
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{Args, Status, Tag}
import io.projectglow.Glow
import io.projectglow.SparkTestShim.{FunSuite, SharedSparkSessionBase}
import io.projectglow.common.{GlowLogging, TestUtils}

abstract class GlowBaseTest
    extends FunSuite
    with SharedSparkSessionBase
    with Eventually
    with GlowLogging
    with GlowTestData
    with TestUtils
    with JenkinsTestPatience {

  override def initializeSession(): Unit = {
    super.initializeSession()
    Glow.register(spark, newSession = false)
    SparkSession.setActiveSession(spark)
    Log.setGlobalLogLevel(Log.LogLevel.ERROR)
  }

  protected def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
      testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" ($param)", testTags: _*)(testFun(param))
    }
  }

  override def afterEach(): Unit = {
    eventually {
      DebugFilesystem.assertNoOpenStreams()
      assert(spark.sparkContext.getPersistentRDDs.isEmpty)
      assert(spark.sharedState.cacheManager.isEmpty, "Cache not empty.")
    }
    super.afterEach()
  }

  override def runTest(testName: String, args: Args): Status = {
    logger.info(s"Running test '$testName'")
    val res = super.runTest(testName, args)
    if (res.succeeds()) {
      logger.info(s"Done running test '$testName'")
    } else {
      logger.info(s"Done running test '$testName' with a failure")
    }
    res
  }

  protected def withSparkConf[T](configs: Map[String, String])(f: => T): T = {
    val initialConfigValues = configs.keys.map(k => (k, spark.conf.getOption(k)))
    try {
      configs.foreach { case (k, v) => spark.conf.set(k, v) }
      f
    } finally {
      initialConfigValues.foreach {
        case (k, Some(v)) => spark.conf.set(k, v)
        case (k, None) => spark.conf.unset(k)
      }
    }
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
