# Databricks notebook source
import glow
spark = glow.register(spark)
from pyspark.sql.functions import *
path = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
df = spark.read.format("vcf").option("flattenInfoFields", True).load(path)

# COMMAND ----------

display(df.drop("genotypes").limit(10))

# COMMAND ----------

display(df.where(col("INFO_SVTYPE").isNotNull()).select("*", glow.expand_struct(glow.call_summary_stats("genotypes"))).drop("genotypes").limit(10))

# COMMAND ----------

display(df.select(glow.expand_struct(glow.hardy_weinberg("genotypes"))).limit(10))