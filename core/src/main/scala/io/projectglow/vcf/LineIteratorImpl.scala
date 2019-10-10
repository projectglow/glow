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
