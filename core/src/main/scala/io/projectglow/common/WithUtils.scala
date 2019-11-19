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

import java.io.Closeable
import java.util.concurrent.locks.Lock

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel

import scala.util.control.NonFatal

object WithUtils {

  /**
   * Scala version of Java's try-with-resources or Python's "with".
   *
   * Execute a block with an object that implements [[java.io.Closeable]]. Makes sure the
   * item is closed when the block is left, even if an exception occurs. Also makes sure any
   * Exceptions during close are treated according to best practice.
   *
   * @param closeable the closeable resource
   * @param func function block to be executed with the resource
   * @tparam T type of the closeable resource
   * @tparam R return type of resource
   * @return the value of the block
   */
  def withCloseable[T <: Closeable, R](closeable: T)(func: T => R): R = {
    var triedToClose = false
    try {
      func(closeable)
    } catch {
      case NonFatal(e) =>
        try {
          closeable.close()
        } catch {
          case NonFatal(t) =>
            e.addSuppressed(t)
        }
        triedToClose = true
        throw e
    } finally {
      // if we haven't tried to close it in the exception handler, try here.
      if (!triedToClose) {
        closeable.close()
      }
    }
  }

  def withLock[T](lock: Lock)(f: => T): T = {
    lock.lock()
    try {
      f
    } finally {
      lock.unlock()
    }
  }

  def withCachedRDD[T, U](rdd: RDD[T])(f: RDD[T] => U): U = {
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      f(rdd)
    } finally {
      rdd.unpersist()
    }
  }

  def withCachedDataset[T, U](ds: Dataset[T])(f: Dataset[T] => U): U = {
    ds.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      f(ds)
    } finally {
      ds.unpersist()
    }
  }
}
