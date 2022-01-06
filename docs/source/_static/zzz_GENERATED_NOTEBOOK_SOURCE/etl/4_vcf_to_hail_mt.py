# Databricks notebook source
# MAGIC %md
# MAGIC ### Convert VCF file to hail matrix table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run notebook(s) to set everything up

# COMMAND ----------

# MAGIC %run ../1_setup_constants_hail

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

method = 'ingest'
test = 'vcf'
library = 'hail'
datetime = datetime.now(pytz.timezone('US/Pacific'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### set spark configurations
# MAGIC 
# MAGIC to avoid `multi part upload` failure sometimes observed with Hail

# COMMAND ----------

spark.conf.set("spark.hadoop.fs.s3a.multipart.threshold", 2097152000)
spark.conf.set("spark.hadoop.fs.s3a.multipart.size", 104857600)
spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", 500)
spark.conf.set("spark.hadoop.fs.s3a.connection.timeout", 600000)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### prepare hail matrix table

# COMMAND ----------

start_time = time.time()

# COMMAND ----------

mt = hl.import_vcf(input_vcf, reference_genome='GRCh37')
mt.show()

# COMMAND ----------

mt.count()

# COMMAND ----------

mt.repartition(n_partitions_hail).write(hail_matrix_table_outpath, overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### log runtime

# COMMAND ----------

end_time = time.time()
log_metadata(datetime, n_samples, n_variants, 0, 0, method, test, library, spark_version, node_type_id, n_workers, start_time, end_time, run_metadata_delta_path)