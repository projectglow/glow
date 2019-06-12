package org.bdgenomics.adam.rdd

import htsjdk.variant.vcf.VCFHeader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.seqdoop.hadoop_bam.util.{VCFHeaderReader, WrapSeekable}

object VCFMetadataLoader {

  // Currently private in ADAM (ADAMContext).
  def readVcfHeader(config: Configuration, pathName: String): VCFHeader = {
    val is = WrapSeekable.openPath(config, new Path(pathName))
    val header = VCFHeaderReader.readHeaderFrom(is)
    is.close()
    header
  }
}
