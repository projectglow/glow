package io.projectglow

import io.projectglow.SparkTestShim.FunSuite
import io.projectglow.sql.{GlowBaseTest, GlowTestData}
import org.apache.spark.sql.test.SharedSparkSession

class SparkSuite extends GlowBaseTest {
  test("spark program") {
    logger.warn("Testing env var " + System.getenv("SPARK_TESTING"))
    logger.warn("Testing java prop " + System.getProperty("spark.testing"))
    logger.warn("Spark Home" + System.getenv("SPARK_HOME"))
    logger.warn("Spark Java home" + System.getProperty("spark.test.home"))
    spark.range(10).count()
  }

  test("read file") {
    spark.read.text(s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf").count()
  }
}
