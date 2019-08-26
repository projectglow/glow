package com.databricks.bgen

import com.google.common.io.LittleEndianDataInputStream
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import com.databricks.hls.common.WithUtils
import com.databricks.hls.sql.util.SerializableConfiguration
import com.databricks.vcf.VariantSchemas

/**
 * Infers the schema of a set of BGEN files from the user-provided options and the header of each
 * file.
 *
 * Currently the implementation is very simple. It checks if sample IDs are defined in either a
 * .sample file or in any of the headers. If so, it returns a fixed schema that includes a
 * `sampleId` genotype field. If not, it returns the same schema without a `sampleId` field.
 */
object BgenSchemaInferrer {
  def inferSchema(
      spark: SparkSession,
      files: Seq[FileStatus],
      options: Map[String, String]): StructType = {
    val sampleIdsFromSampleFile =
      BgenFileFormat.getSampleIds(options, spark.sparkContext.hadoopConfiguration)
    if (sampleIdsFromSampleFile.isDefined) {
      return VariantSchemas.bgenDefaultSchema(hasSampleIds = true)
    }

    val serializableConf = new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    val ignoreExtension = options.get(BgenFileFormat.IGNORE_EXTENSION_KEY).exists(_.toBoolean)
    val bgenFiles =
      files.filter { fs =>
        fs.getLen > 0 && (fs
          .getPath
          .toString
          .endsWith(BgenFileFormat.BGEN_SUFFIX) || ignoreExtension)
      }
    val hasSampleIds = spark
      .sparkContext
      .parallelize(bgenFiles)
      .map { fs =>
        val hadoopFs = fs.getPath.getFileSystem(serializableConf.value)
        WithUtils.withCloseable(hadoopFs.open(fs.getPath)) { stream =>
          val littleEndianDataInputStream = new LittleEndianDataInputStream(stream)
          new BgenHeaderReader(littleEndianDataInputStream)
            .readHeader(None)
            .sampleIds
            .exists(_.nonEmpty)
        }
      }
      .collect()
      .exists(identity)

    VariantSchemas.bgenDefaultSchema(hasSampleIds)
  }
}
