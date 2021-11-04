# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Pipeline runs comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

run_info = spark.read.format("delta").load(run_metadata_delta_path) \
                                     .withColumnRenamed("worker_type", "n_workers") \
                                     .sort(fx.col("datetime").desc())
display(run_info)

# COMMAND ----------

run_info_pd = run_info.toPandas()
run_info_pd

# COMMAND ----------

#TODO
#add vizualisations! 
#Add benchmarks for each step (ingest, QC etc.)