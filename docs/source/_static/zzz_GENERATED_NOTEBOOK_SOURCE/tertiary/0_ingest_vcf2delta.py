# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Ingest VCF into delta
# MAGIC 
# MAGIC We already have the data in delta, so this is notebook is for benchmarking purposes

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run notebook(s) to set everything up

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read VCF

# COMMAND ----------

method = 'ingest'
test = 'vcf'
library = 'glow'
datetime = datetime.now(pytz.timezone('US/Pacific'))

# COMMAND ----------

start_time = time.time()

# COMMAND ----------

spark.read.format("vcf").load(output_vcf) \
                        .write \
                        .format("delta") \
                        .mode("overwrite") \
                        .save(output_delta_tmp)

# COMMAND ----------

end_time = time.time()
runtime = float("{:.2f}".format((end_time - start_time)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read delta table in and count

# COMMAND ----------

spark.read.format("delta").load(output_delta_tmp).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### log metadata

# COMMAND ----------

l = [(datetime, n_samples, n_variants, n_covariates, n_phenotypes, method, test, library, spark_version, node_type_id, n_workers, runtime)]
run_metadata_delta_df = spark.createDataFrame(l, schema=schema)
run_metadata_delta_df.write.mode("append").format("delta").save(run_metadata_delta_path)