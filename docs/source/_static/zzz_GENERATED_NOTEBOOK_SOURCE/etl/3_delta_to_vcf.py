# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Convert Delta Table to VCF
# MAGIC 
# MAGIC Take the delta table and write it back to VCF (for input to Hail)
# MAGIC 
# MAGIC note: most of the job writing back to a single VCF is single threaded, 
# MAGIC 
# MAGIC but it requires the full dataset to be read into memory on a spark cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run notebook(s) to set everything up

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

spark.read.format("delta").load(output_delta) \
                          .limit(100) \
                          .write \
                          .mode("overwrite") \
                          .format("bigvcf") \
                          .save(output_vcf_small)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check output VCF

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n 100 $output_vcf

# COMMAND ----------

# MAGIC %md
# MAGIC ##### write vcf on full dataset

# COMMAND ----------

spark.read.format("delta").load(output_delta) \
                          .repartition(n_partitions) \
                          .write \
                          .mode("overwrite") \
                          .format("bigvcf") \
                          .save(output_vcf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check output

# COMMAND ----------

delta_vcf = spark.read.format("vcf").load(output_vcf)

# COMMAND ----------

delta_vcf.count()