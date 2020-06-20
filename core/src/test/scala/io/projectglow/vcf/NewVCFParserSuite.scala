package io.projectglow.vcf

import io.projectglow.sql.GlowBaseTest

class NewVCFParserSuite extends GlowBaseTest {
  test("new parser") {
    import org.apache.spark.sql.functions._
    val vcf = s"$testDataHome/1000G.phase3.broad.withGenotypes.chr20.10100000.vcf"
    val df = spark.read.format("vcf").load(vcf)
      .withColumn("genotype", expr("genotypes[0]"))
//    val df = spark.read.format("vcf").load(vcf).selectExpr("expand_struct(genotypes[0])")
    df.printSchema()
    df.show()
  }

}
