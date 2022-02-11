# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Simulate pVCF
# MAGIC 
# MAGIC Uses the 1000 genomes to simulate a project-level VCF at a larger scale and write to delta lake
# MAGIC 
# MAGIC - Please download 1000G data first and write to Delta Lake
# MAGIC   - see `./data/download_1000G` (notebook in [github](https://github.com/projectglow/glow/tree/master/docs/source/_static/notebooks/etl/data/download_1000G.html))
# MAGIC - For now we manually define functions to handle hardy-weinberg allele frequency and multiallelic variants
# MAGIC   - these functions are in `./python/functions` (notebook in [github](https://github.com/projectglow/glow/tree/master/docs/source/_static/notebooks/etl/python/functions.html))
# MAGIC   
# MAGIC _TODO_ use sim1000G to get realistic families to test offset correction

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %run ./python/functions

# COMMAND ----------

vcf = spark.read.format("delta").load(output_vcf_delta)
total_variants = vcf.count()
fraction = n_variants / total_variants

# COMMAND ----------

display(vcf)

# COMMAND ----------

simulated_vcf = vcf.sample(withReplacement=False, fraction=fraction) \
                   .repartition(n_partitions) \
                   .withColumn("genotypes", simulate_genotypes_udf(fx.col("INFO_AF"), 
                                                                   fx.lit(n_samples)))

# COMMAND ----------

simulated_vcf.count()

# COMMAND ----------

display(simulated_vcf.drop("genotypes"))

# COMMAND ----------

simulated_vcf.write.mode("overwrite").format("delta").save(output_simulated_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check output delta table

# COMMAND ----------

delta_vcf = spark.read.format("delta").load(output_simulated_delta).drop("genotypes")

# COMMAND ----------

display(delta_vcf)

# COMMAND ----------

display(delta_vcf.groupBy("contigName").count())