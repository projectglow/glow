# Databricks notebook source
import hail as hl
hl.init(sc, idempotent=True, quiet=True)

import glow
spark = glow.register(spark)
from glow.hail import functions

from pyspark.sql.functions import *

# COMMAND ----------

vcf_path = '/databricks-datasets/hail/data-001/1kg_sample.vcf.bgz'
out_path = 'dbfs:/tmp/1kg_sample.delta'

# COMMAND ----------

vcf_mt = hl.import_vcf(vcf_path)
vcf_mt.show()

# COMMAND ----------

vcf_mt.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### convert to spark dataframe with glow schema

# COMMAND ----------

df = functions.from_matrix_table(vcf_mt, include_sample_ids=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format("delta").save(out_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read back in, view and count dataframe

# COMMAND ----------

df2 = spark.read.format("delta").load(out_path)

# COMMAND ----------

display(df2)

# COMMAND ----------

df2.count()

# COMMAND ----------

dbutils.fs.rm(out_path, recurse=True)