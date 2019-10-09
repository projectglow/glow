package org.projectglow.common

import htsjdk.samtools.ValidationStringency

trait HasStringency extends GlowLogging {
  def stringency: ValidationStringency
  protected def provideWarning(warning: String): Unit = {
    if (stringency == ValidationStringency.STRICT) {
      throw new IllegalArgumentException(warning)
    } else if (stringency == ValidationStringency.LENIENT) {
      logger.warn(warning)
    }
  }
}
