# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Simulate firth regression genetic association study
# MAGIC 
# MAGIC Uses a simulated project-level VCF from chromosome 22 of the 1000 genomes

# COMMAND ----------

# MAGIC %md
# MAGIC ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

method = 'logistic'
test = 'approx-firth'
library = 'glow'
datetime = datetime.now(pytz.timezone('US/Pacific'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### load genotypes

# COMMAND ----------

delta_vcf = spark.read.format("delta").load(input_delta)

# COMMAND ----------

delta_vcf.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### load simulated covariates, phentoypes, and offsets

# COMMAND ----------

covariates = pd.read_csv(covariates_path, dtype={'sample_id': str}, index_col='sample_id')
covariates.head(5)

# COMMAND ----------

phenotypes = pd.read_csv(binary_phenotypes_path, dtype={'sample_id': str}, index_col='sample_id')
phenotypes.head(5)

# COMMAND ----------

offsets = pd.read_csv(binary_y_hat_path, dtype={'sample_id': str}).set_index(['sample_id'])
offsets.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### run approx firth logistic regression gwas

# COMMAND ----------

contigs = ['21', '22']

# COMMAND ----------

start_time = time.time()


for num, contig in enumerate(contigs):
  offsets_chr = offsets[offsets['contigName'] == contig].drop(['contigName'], axis=1) 
  results = glow.gwas.logistic_regression(
    delta_vcf.where(fx.col('contigName') == contig),
    phenotypes,
    covariates,
    offsets_chr,
    values_column='values',
    correction='approx-firth',
    pvalue_threshold=0.05,
    # In addition to filtering the DataFrame, hint to Glow that the input only contains one contig
    contigs=[contig])
  
  mode = 'overwrite' if num == 0 else 'append'
  results.write.format('delta'). \
                mode(mode). \
                save(binary_gwas_results_path)
  
end_time = time.time()
runtime = float("{:.2f}".format((end_time - start_time)))

# COMMAND ----------

results_df = spark.read.format("delta").load(binary_gwas_results_path). \
                                        withColumn("log_p", -fx.log10("pvalue")).cache()
n_filtered_variants = results_df.count()
display(results_df.orderBy("pvalue"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### log metadata

# COMMAND ----------

l = [(datetime, n_samples, n_filtered_variants, n_covariates, n_binary_phenotypes, method, test, library, spark_version, node_type_id, n_workers, runtime)]
run_metadata_delta_df = spark.createDataFrame(l, schema=schema)
run_metadata_delta_df.write.mode("append").format("delta").save(run_metadata_delta_path)