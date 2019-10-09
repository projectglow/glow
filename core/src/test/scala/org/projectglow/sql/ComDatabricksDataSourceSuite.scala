package org.projectglow.sql

import java.nio.file.{Files, Path}

// Sanity check that legacy DataSource names starting with "com.databricks." still work
class ComDatabricksDataSourceSuite extends GlowBaseTest {

  lazy val vcf = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
  lazy val bgen = s"$testDataHome/bgen/example.16bits.bgen"

  protected def createTempPath(extension: String): Path = {
    val tempDir = Files.createTempDirectory(s"test-$extension-dir")
    val path = tempDir.resolve(s"test.$extension")
    logger.info(s"Writing $extension to path ${path.toAbsolutePath.toString}")
    path
  }

  case class DataSources(legacyDataSource: String, standardDataSource: String, file: String)

  // Legacy read DataSource, standard read DataSource, file
  val readDataSources: Seq[DataSources] = Seq(
    DataSources("com.databricks.vcf", "vcf", vcf),
    DataSources("com.databricks.bgen", "bgen", bgen)
  )

  gridTest("read")(readDataSources) { rds =>
    val legacyDf =
      spark.read.format(rds.legacyDataSource).load(rds.file).orderBy("contigName", "start")
    val standardDf =
      spark.read.format(rds.standardDataSource).load(rds.file).orderBy("contigName", "start")
    assert(legacyDf.collect sameElements standardDf.collect)
  }

  // Legacy write source, standard read DataSource, file
  val writeDataSources: Seq[DataSources] = Seq(
    DataSources("com.databricks.vcf", "vcf", vcf),
    DataSources("com.databricks.bigvcf", "vcf", vcf),
    DataSources("com.databricks.bigbgen", "bgen", bgen)
  )

  gridTest("write")(writeDataSources) { wds =>
    val inputDf =
      spark.read.format(wds.standardDataSource).load(wds.file).orderBy("contigName", "start")
    val rewrittenFile = createTempPath(wds.standardDataSource).toString
    inputDf.write.format(wds.legacyDataSource).save(rewrittenFile)
    val rewrittenDf =
      spark.read.format(wds.standardDataSource).load(rewrittenFile).orderBy("contigName", "start")
    assert(inputDf.collect sameElements rewrittenDf.collect)
  }
}
