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

package io.projectglow.vcf

import htsjdk.tribble.readers.LineIterator

class LineIteratorImpl(iter: Iterator[String]) extends LineIterator with Iterator[String] {
  var peekOpt: Option[String] = None

  override def peek(): String = {
    if (peekOpt.isEmpty) {
      if (!iter.hasNext) {
        return null
      }
      peekOpt = Some(iter.next())
    }
    peekOpt.get
  }

  override def next(): String = {
    val nextValue = if (peekOpt.isDefined) {
      peekOpt.get
    } else {
      iter.next()
    }
    peekOpt = None
    nextValue
  }

  override def hasNext: Boolean = {
    iter.hasNext
  }
}
