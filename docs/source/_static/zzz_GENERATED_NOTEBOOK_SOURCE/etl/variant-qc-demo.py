# Databricks notebook source
import glow
glow.register(spark)
from pyspark.sql.functions import *
path = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
df = spark.read.format("vcf").option("flattenInfoFields", True).load(path)

# COMMAND ----------

display(df.drop("genotypes").limit(10))

# COMMAND ----------

display(df.where(expr("INFO_SVTYPE is not null")).selectExpr("*", "expand_struct(call_summary_stats(genotypes))").drop("genotypes").limit(10))

# COMMAND ----------

display(df.selectExpr("expand_struct(hardy_weinberg(genotypes))").limit(10))

# COMMAND ----------

display(df.selectExpr("expand_struct(dp_summary_stats(genotypes))"))

# COMMAND ----------

display(df.selectExpr("expand_struct(dp_summary_stats(genotypes))"))