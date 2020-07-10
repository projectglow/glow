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

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer

import org.apache.spark.ml.linalg.DenseMatrix

object PythonUtils {
  def doubleArrayFromBytes(len: Int, bytes: Array[Byte]): Array[Double] = {
    val buffer = ByteBuffer.wrap(bytes)
    val out = new Array[Double](len)
    var i = 0
    while (i < len) {
      out(i) = buffer.getDouble()
      i += 1
    }
    out
  }
}
