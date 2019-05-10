package com.databricks.hls.common

import java.io.Closeable
import java.util.concurrent.locks.Lock

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
      if (!triedToClose)
        closeable.close()
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
}
