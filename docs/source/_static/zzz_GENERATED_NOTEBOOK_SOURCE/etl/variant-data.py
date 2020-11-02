# Databricks notebook source
# DBTITLE 1,Change these paths to point to your data files
import glow
glow.register(spark)
from pyspark.sql.functions import *
vcf_path = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
bgen_path = "/databricks-datasets/genomics/1kg-bgens/1kg_chr22.bgen"

# COMMAND ----------

# DBTITLE 1,Read in a VCF with flatten INFO fields and with sample IDs
vcf_df = spark.read.format("vcf")\
  .load(vcf_path)\
  .drop("genotypes")

display(vcf_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Read in a VCF with flattenInfoFields and includeSampleIds disabled
vcf_df_unflattenedInfo_noSampleId = spark.read.format("vcf")\
  .option("includeSampleIds", False)\
  .option("flattenInfoFields", False)\
  .load(vcf_path)\
  .withColumn("first_genotype", expr("genotypes[0]"))\
  .drop("genotypes")

display(vcf_df_unflattenedInfo_noSampleId.limit(10))

# COMMAND ----------

# DBTITLE 1,Read in a BGEN file with the same schema
df = spark.read.format("bgen").load(bgen_path)

display(df.withColumn("firstGenotype", expr("genotypes[0]")).drop("genotypes").limit(10))

# COMMAND ----------

# DBTITLE 1,Combine data from VCF and BGEN files
bgen_df = spark.read.format("bgen").schema(vcf_df.schema).load(bgen_path)
merged_df = vcf_df.union(bgen_df)
display(merged_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Save to a Delta table
merged_df.write.format("delta").mode("overwrite").save("/tmp/variants_delta")

# COMMAND ----------

# DBTITLE 1,Save a subset of the data as a VCF
merged_df.limit(100).repartition(1).write.mode("overwrite").format("vcf").save("/tmp/vcf_output")