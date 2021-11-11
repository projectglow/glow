# Databricks notebook source
# MAGIC %md
# MAGIC ### Run Hail linear regression

# COMMAND ----------

# MAGIC %md
# MAGIC ##### set constants

# COMMAND ----------

# MAGIC %run ../1_setup_constants_hail

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

method = 'linear'
test = 'linear_regression'
library = 'hail'
datetime = datetime.now(pytz.timezone('US/Pacific'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### load data

# COMMAND ----------

covariate_df = spark.createDataFrame(pd.read_csv(covariates_path).set_index("sample_id", drop=False)). \
                     withColumn("sample_id", fx.col("sample_id").cast(StringType()))
covariate_df.columns[1:]

# COMMAND ----------

phenotype_df = spark.createDataFrame(pd.read_csv(quantitative_phenotypes_path).set_index("sample_id", drop=False)). \
                     withColumn("sample_id", fx.col("sample_id").cast(StringType()))

# COMMAND ----------

combined_phe_covs_df = phenotype_df.join(covariate_df, "sample_id").withColumnRenamed("sample_id", "s")
combined_phe_covs_ht = hl.Table.from_spark(combined_phe_covs_df).key_by("s")

# COMMAND ----------

mt = hl.read_matrix_table(hail_matrix_table)
mt.show(5)

# COMMAND ----------

mt = mt.annotate_cols(**combined_phe_covs_ht[mt.s])
mt.describe()

# COMMAND ----------

mt.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run linear regression

# COMMAND ----------

start_time = time.time()
phenotypes = phenotype_df.columns[1:]
y = list(map(lambda p: hl.float64(mt[p]), phenotypes))
covs = list(map(lambda cov: mt[cov], covariate_df.columns[1:]))
x = mt.GT.n_alt_alleles()
hl_res = hl.methods.linear_regression_rows(y,x,[1]+covs)
hl_res.checkpoint(quantitative_gwas_results_chk, overwrite=True)
n_filtered_variants = hl_res.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### log runtime

# COMMAND ----------

end_time = time.time()
log_metadata(datetime, n_samples, n_filtered_variants, n_covariates, n_quantitative_phenotypes, method, test, library, spark_version, node_type_id, n_workers, start_time, end_time, run_metadata_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read results back in and count

# COMMAND ----------

hl_res = hl.read_table(quantitative_gwas_results_chk)
hl_res.describe()

# COMMAND ----------

hl_res.count()