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
# MAGIC 
# MAGIC The majority of compute is spent on `ingest`, `etl`, and `quality control`, but these steps are only run one or a few times at scale.
# MAGIC 
# MAGIC `Regressions` (or other analytics, such as emprically deriving P values from simulations) are typically run many times.
# MAGIC 
# MAGIC TODO: flame graph of the executors
# MAGIC 
# MAGIC where are the executors spending their time
# MAGIC 
# MAGIC garbage collection when dealing with arrays, convert ints to shorts
# MAGIC 
# MAGIC transforming large arrays into smaller versions
# MAGIC 
# MAGIC sample filtering biggest bottleneck.
# MAGIC For regressions can pass in sample Ids, under the hood does the masking of the samples
# MAGIC For offset generation it is more complicated. But this is on a smaller dataset, ~500k variants.
# MAGIC But you have to do it per phenotype. So it adds up if you do a lot of phenotypes.

# COMMAND ----------

display(benchmark_results.where(fx.col("library") == ("glow")). \
                          sort(fx.col("method"), fx.col("core_hours"), fx.col("test"))
       )