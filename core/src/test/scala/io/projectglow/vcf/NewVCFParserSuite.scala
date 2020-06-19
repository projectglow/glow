package io.projectglow.vcf

import io.projectglow.sql.GlowBaseTest

class NewVCFParserSuite extends GlowBaseTest {
  test("new parser") {
    import org.apache.spark.sql.functions._
    val vcf = s"$testDataHome/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf"
//    val df = spark.read.format("vcf").load(vcf)
//      .withColumn("genotype", expr("genotypes[0]"))
    val df = spark.read.format("vcf").load(vcf).selectExpr("expand_struct(genotypes[0])")
    df.printSchema()
    df.show()
  }

}
