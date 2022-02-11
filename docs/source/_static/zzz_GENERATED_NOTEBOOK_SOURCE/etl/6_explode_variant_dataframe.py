# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Explode variant dataframe
# MAGIC 
# MAGIC Take a variant dataframe in a pVCF representation (one variant with all genotypes)
# MAGIC   - genotypes for each variant represented as an array of structs
# MAGIC 
# MAGIC And explode it so each row contains one variant and one genotype

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

method = 'etl'
test = 'explode_genotypes_array'
library = 'glow'
datetime = datetime.now(pytz.timezone('US/Pacific'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### load data

# COMMAND ----------

vcf_df = spark.read.format("delta").load(output_simulated_delta)

# COMMAND ----------

vcf_df.count()

# COMMAND ----------

display(vcf_df.drop("genotypes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### explode genotypes

# COMMAND ----------

start_time = time.time()

# COMMAND ----------

explode_vcf_df = vcf_df.select("contigName", "start", "end", "referenceAllele", "alternateAlleles", "qual",
   fx.explode(
      fx.arrays_zip(fx.col("genotypes.sampleId").alias("sampleId"), 
                    fx.col("genotypes.calls").alias("calls"))
   ).alias("genotypes")
). \
  withColumn("sampleId", fx.col("genotypes.sampleId")). \
  withColumn("calls", fx.col("genotypes.calls")). \
  withColumn("qual", fx.rand(seed=42)*100). \
  drop("genotypes")

# COMMAND ----------

explode_vcf_df.write.mode("overwrite").format("delta").save(output_exploded_delta)

# COMMAND ----------

end_time = time.time()
log_metadata(datetime, n_samples, n_variants, 0, 0, method, test, library, spark_version, node_type_id, n_workers, start_time, end_time, run_metadata_delta_path)

# COMMAND ----------

explode_vcf_df = spark.read.format("delta").load(output_exploded_delta)
explode_vcf_df.count()

# COMMAND ----------

display(explode_vcf_df)

# COMMAND ----------

display(explode_vcf_df)