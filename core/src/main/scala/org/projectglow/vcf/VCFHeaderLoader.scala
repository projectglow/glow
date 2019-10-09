package org.projectglow.vcf

import htsjdk.variant.vcf.VCFHeader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bdgenomics.adam.rdd.VCFMetadataLoader

object VCFHeaderLoader {

  /**
   * Reads VCF headers from a path, which can be a single file, a sharded file, or a glob of
   * multiple files. When reading from a directory, lists the files, discards any hidden files
   * (filenames starting with "_", often a "SUCCESS" file in a sharded file; then reads the VCF
   * header from the first file. Note that this assumes the directory is a sharded file and does
   * not contain multiple sample IDs.
   */
  def readVcfHeaderFromGlob(config: Configuration, pathName: String): VCFHeader = {
    val path = new Path(pathName)
    val fileSys = path.getFileSystem(config)

    val fileStatusList = fileSys.globStatus(path).toList
    val pathList = fileStatusList.flatMap { fs =>
      if (fs.isDirectory) {
        val dirContents = fileSys.listStatus(fs.getPath).map(_.getPath).toList
        dirContents.find(!_.getName.startsWith("_"))
      } else {
        Some(fs.getPath)
      }
    }

    VCFMetadataLoader.readVcfHeader(config, pathList.head.toString)
  }
}
