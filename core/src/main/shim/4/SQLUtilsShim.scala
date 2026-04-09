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

package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.types.StructType

// Spark 4 shim for SQLUtils methods that differ between Spark versions.
// In Spark 4, SparkSession is an abstract class and internalCreateDataFrame/extensions
// are only on the classic.SparkSession subclass.
private[sql] object SQLUtilsShim {

  def internalCreateDataFrame(
      sess: SparkSession,
      catalystRows: RDD[InternalRow],
      schema: StructType,
      isStreaming: Boolean): DataFrame = {
    sess.asInstanceOf[ClassicSparkSession].internalCreateDataFrame(catalystRows, schema, isStreaming)
  }

  def getSessionExtensions(session: SparkSession): SparkSessionExtensions = {
    session.asInstanceOf[ClassicSparkSession].extensions
  }
}
