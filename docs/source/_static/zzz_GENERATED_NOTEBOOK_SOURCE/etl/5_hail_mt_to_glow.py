# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Convert Hail Matrix Table to Glow Schema
# MAGIC 
# MAGIC Run using the cluster configuration detailed [here](https://github.com/projectglow/glow/pull/381/files)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run notebook(s) to set everything up

# COMMAND ----------

# MAGIC %run ../1_setup_constants_hail

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

method = 'etl'
test = 'hail_to_glow'
library = 'glow'
datetime = datetime.now(pytz.timezone('US/Pacific'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read in matrix table

# COMMAND ----------

mt = hl.read_matrix_table(hail_matrix_table_outpath)
mt.show(5)

# COMMAND ----------

mt.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### convert to spark dataframe with glow schema

# COMMAND ----------

start_time = time.time()
df = functions.from_matrix_table(mt, include_sample_ids=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format("delta") \
        .mode("overwrite") \
        .save(delta_table_outpath)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### log runtime

# COMMAND ----------

end_time = time.time()
log_metadata(datetime, n_samples, n_variants, 0, 0, method, test, library, spark_version, node_type_id, n_workers, start_time, end_time, run_metadata_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read back in, view and count dataframe

# COMMAND ----------

df2 = spark.read.format("delta").load(delta_table_outpath)
display(df2.drop("genotypes"))

# COMMAND ----------

df2.count()