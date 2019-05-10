package org.bdgenomics.adam.rdd

import htsjdk.variant.vcf.{VCFHeader, VCFHeaderLine}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.Sample
import org.seqdoop.hadoop_bam.util.{VCFHeaderReader, WrapSeekable}

object VCFMetadataLoader {

  // Currently private in ADAM (ADAMContext)
  def loadVcfMetadata(
      sc: SparkContext,
      pathName: String): (SequenceDictionary, Seq[Sample], Seq[VCFHeaderLine]) = {

    sc.loadVcfMetadata(pathName)
  }

  // Currently private in ADAM (ADAMContext)
  def readVcfHeader(sc: SparkContext, pathName: String): VCFHeader = {
    val is = WrapSeekable.openPath(sc.hadoopConfiguration, new Path(pathName))
    val header = VCFHeaderReader.readHeaderFrom(is)
    is.close()
    header
  }
}
