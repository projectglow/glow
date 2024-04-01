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

package io.projectglow

// Spark 3.4 APIs that are not inter-version compatible
object SparkTestShim extends SparkTestShimBase {
<<<<<<< HEAD:core/src/test/shim/3.4/SparkTestShim.scala
  // [SPARK-28744][SQL][TEST] rename SharedSQLContext to SharedSparkSession
  // Renames SharedSparkSession to SharedSparkSessionBase
  override type SharedSparkSessionBase = org.apache.spark.sql.test.SharedSparkSessionBase
  // Scalatest renames FunSuite to AnyFunSuite
  override type FunSuite = org.scalatest.funsuite.AnyFunSuite
=======
  // Spark renames SharedSparkSession to SharedSparkSessionBase
  override type SharedSparkSessionBase = org.apache.spark.sql.test.SharedSparkSession
  // Scalatest renames FunSuite to AnyFunSuite
  override type FunSuite = org.scalatest.FunSuite
>>>>>>> f6791fc (Fetch upstream):core/src/test/shim/2.4/SparkTestShim.scala
}
