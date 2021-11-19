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

# MAGIC %md
# MAGIC ##### load benchmark metadata

# COMMAND ----------

benchmark_metadata = spark.read.format("delta").load(run_metadata_delta_path). \
                                                sort(fx.col("datetime").desc(), "runtime", "n_samples"). \
                                                withColumn("core_hours", fx.round((fx.col("n_cores") * fx.col("runtime")) / 3600, 3))

# COMMAND ----------

benchmark_results = benchmark_metadata.groupBy(benchmark_metadata.columns[:-2]). \
                                       agg(fx.mean(fx.col("runtime")).alias("runtime"), fx.mean(fx.col("core_hours")).alias("core_hours")). \
                                       sort(fx.col("datetime"))
display(benchmark_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Ingest: Glow vs Hail

# COMMAND ----------

display(benchmark_results.where(fx.col("method") == "ingest"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Regression: Glow vs Hail

# COMMAND ----------

display(benchmark_results.where((fx.col("test") == ("linear_regression")) | 
                                (fx.col("test").rlike("firth")))
       )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC TODO, add other steps in the pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Glow pipeline steps breakdown

# COMMAND ----------

display(benchmark_results.where(fx.col("library") == ("glow")). \
                          sort(fx.col("method"), fx.col("core_hours"), fx.col("test"))
       )