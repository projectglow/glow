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

package io.projectglow.sql.util

import java.io.Closeable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
 * Utility wrapper functions.
 */
trait WithUtils {
  def withCachedRDD[T, U](rdd: RDD[T])(f: RDD[T] => U): U = {
    rdd.cache()
    try {
      f(rdd)
    } finally {
      rdd.unpersist()
    }
  }

  def withCachedDataset[T, U](ds: Dataset[T])(f: Dataset[T] => U): U = {
    ds.cache()
    try {
      f(ds)
    } finally {
      ds.unpersist()
    }
  }

  def withStream[Stream <: Closeable, T](stream: Stream)(f: Stream => T): T = {
    try {
      f(stream)
    } finally {
      stream.close()
    }
  }
}
