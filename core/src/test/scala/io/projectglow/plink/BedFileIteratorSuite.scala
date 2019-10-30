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

package io.projectglow.plink

import java.io.IOException

import com.google.common.io.LittleEndianDataInputStream
import org.apache.hadoop.fs.Path

import io.projectglow.sql.GlowBaseTest

class BedFileIteratorSuite extends GlowBaseTest {
  val bedPath = s"$testDataHome/plink/five-samples-five-variants/bed-bim-fam/test.bed"
  val numMagicBytes = 3
  val numSamples = 5
  val blockSize = 2
  val calls: Seq[Array[Array[Int]]] = Seq(
    Array(Array(0, 0), Array(0, 1), Array(1, 1), Array(-1, -1), Array(-1, -1)),
    Array(Array(0, 0), Array(0, 1), Array(0, 0), Array(0, 1), Array(0, 1)),
    Array(Array(0, 0), Array(0, 1), Array(0, 0), Array(0, 1), Array(0, 1)),
    Array(Array(0, 0), Array(0, 1), Array(0, 0), Array(0, 1), Array(0, 1)),
    Array(Array(0, 0), Array(0, 1), Array(0, 0), Array(0, 1), Array(0, 1))
  )

  def compareCalls(from: Int, until: Int): Unit = {
    val p = new Path(bedPath)
    val fs = p.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val baseStream = fs.open(p)
    val stream = new LittleEndianDataInputStream(baseStream)

    baseStream.seek(numMagicBytes + blockSize * from)
    val iterator = new BedFileIterator(stream, baseStream, numSamples, until - from, blockSize)
    iterator.zip(calls.slice(from, until).toIterator).foreach {
      case (i, c) =>
        assert(i.map(_.toSeq).toSeq == c.map(_.toSeq).toSeq)
    }

    val e = intercept[IOException](stream.readByte())
    assert(e.getMessage.contains("Stream is closed"))
  }

  test("From beginning") {
    compareCalls(0, 5)
  }

  test("Slice") {
    compareCalls(1, 3)
  }
}
