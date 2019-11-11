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

package io.projectglow.common

import java.io.{FileInputStream, IOException}

import com.google.common.util.concurrent.Striped

import io.projectglow.common.WithUtils._
import io.projectglow.sql.GlowBaseTest

class WithUtilsSuite extends GlowBaseTest {

  val txtFile = s"$testDataHome/saige_output.txt"

  test("closes after successful function") {
    val stream = new FileInputStream(txtFile)
    withCloseable(stream) { s =>
      s.read() // Should succeed
    }
    assertThrows[IOException](stream.read())
  }

  test("closes after failed function") {
    val stream = new FileInputStream(txtFile)
    val e = intercept[IllegalArgumentException] {
      withCloseable(stream) { s =>
        s.read()
        throw new IllegalArgumentException("Non fatal exception")
      }
    }
    assert(e.getMessage.contains("Non fatal exception"))
    assertThrows[IOException](stream.read())
  }

  test("unlocks after successful function") {
    val striped = Striped.readWriteLock(10)
    val readLock = striped.get(txtFile).readLock()
    val writeLock = striped.get(txtFile).writeLock()
    withLock(readLock) {
      assert(!writeLock.tryLock())
    }
    assert(writeLock.tryLock())
    writeLock.unlock()
  }

  test("unlocks after failed function") {
    val striped = Striped.readWriteLock(10)
    val readLock = striped.get(txtFile).readLock()
    val writeLock = striped.get(txtFile).writeLock()
    val e = intercept[IllegalArgumentException] {
      withLock(readLock) {
        throw new IllegalArgumentException("Non fatal exception")
      }
    }
    assert(e.getMessage.contains("Non fatal exception"))
    assert(writeLock.tryLock())
    writeLock.unlock()
  }

  test("uncaches RDD after successful function") {
    val rdd = spark.sparkContext.parallelize(Seq(1, 2))
    withCachedRDD(rdd) { r =>
      assert(r.count == 2)
      assert(spark.sparkContext.getPersistentRDDs.size == 1)
    }
    assert(spark.sparkContext.getPersistentRDDs.isEmpty)
  }

  test("uncaches RDD after failed function") {
    val rdd = spark.sparkContext.parallelize(Seq(1, 2))
    val e = intercept[IllegalArgumentException] {
      withCachedRDD(rdd) { _ =>
        throw new IllegalArgumentException("Non fatal exception")
      }
    }
    assert(e.getMessage.contains("Non fatal exception"))
    assert(spark.sparkContext.getPersistentRDDs.isEmpty)
  }

  test("uncaches Dataset after successful function") {
    val sess = spark
    import sess.implicits._

    val ds = spark.sparkContext.parallelize(Seq(1, 2)).toDF
    withCachedDataset(ds) { d =>
      assert(d.count == 2)
      assert(!spark.sharedState.cacheManager.isEmpty)
    }
    assert(spark.sharedState.cacheManager.isEmpty)
  }

  test("uncaches Dataset after failed function") {
    val sess = spark
    import sess.implicits._

    val ds = spark.sparkContext.parallelize(Seq(1, 2)).toDF
    val e = intercept[IllegalArgumentException] {
      withCachedDataset(ds) { _ =>
        throw new IllegalArgumentException("Non fatal exception")
      }
    }
    assert(e.getMessage.contains("Non fatal exception"))
    assert(spark.sharedState.cacheManager.isEmpty)
  }
}
