# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> + <img src="https://www.regeneron.com/Content/images/science/regenron.png" alt="logo" width="240"/>
# MAGIC 
# MAGIC ### <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> GloWGR: whole genome regression
# MAGIC 
# MAGIC ### Quantitative phenotypes
# MAGIC 
# MAGIC Recommended cluster setup: 
# MAGIC large memory optimized virtual machines

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

method = 'linear'
test = 'glowgr'
library = 'glow'
datetime = datetime.now(pytz.timezone('US/Pacific'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load data
# MAGIC 
# MAGIC for whole genome regression only 500k independent variants that are not in linkage disequilibrium with each other are required.
# MAGIC 
# MAGIC (~1/40th of the total variants tested for association)

# COMMAND ----------

spark.read.format('delta').load(variants_path). \
                           sample(withReplacement=False, fraction=wgr_fraction, seed=3). \
                           write.mode("overwrite").format("delta"). \
                           save(variants_fraction_path)

# COMMAND ----------

start_time_wgr = time.time()
variant_df = spark.read.format('delta').load(variants_fraction_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Extract sample IDs from a variant DataFrame with `glow.wgr.get_sample_ids`.

# COMMAND ----------

sample_ids = glow.wgr.get_sample_ids(variant_df)
len(sample_ids)

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC 
# MAGIC Create the beginning block genotype matrix and sample block ID mapping with `glow.wgr.block_variants_and_samples`.
# MAGIC 
# MAGIC Write the block matrix to Delta and the sample blocks a JSON file so that we can reuse them for multiple phenotype batches.

# COMMAND ----------

block_df, sample_blocks = glow.wgr.block_variants_and_samples(variant_df, 
                                                     sample_ids, 
                                                     variants_per_block, 
                                                     sample_block_count)

# COMMAND ----------

with open(quantitative_sample_blocks_path, 'w') as f:
  json.dump(sample_blocks, f)
block_df.write.format('delta').mode('overwrite').save(quantitative_block_matrix_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Run whole genome regression (WGR) to calculate expected phenotypes

# COMMAND ----------

block_df = spark.read.format('delta').load(quantitative_block_matrix_path)
with open(quantitative_sample_blocks_path, 'r') as f:
  sample_blocks = json.load(f)

# COMMAND ----------

display(block_df.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Read simulated phenotypes and covariates data from cloud storage (S3 or ADLS) as though it were on the local filesystem via the Databricks file system (DBFS). The phenotypes and covariates should have no missing values.

# COMMAND ----------

label_df = pd.read_csv(quantitative_phenotypes_path, 
                       dtype={'sample_id': str}, 
                       index_col='sample_id')[['QP1']]
label_df.index = label_df.index.map(str) #the integer representation of sample_id was causing issues

# COMMAND ----------

label_df.head(5)

# COMMAND ----------

covariate_df = pd.read_csv(covariates_path, 
                           dtype={'sample_id': str}, 
                           index_col='sample_id')
covariate_df.index = covariate_df.index.map(str) #ensures sample_ids are not represented as integers
covariate_df.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC GloWGR runs best on small batches of phenotypes. Break the phenotype DataFrame into column chunks and generate
# MAGIC leave-one-chromosome-out (LOCO) phenotype predictions for each chunk.

# COMMAND ----------

def chunk_columns(df, chunk_size):
  for start in range(0, df.shape[1], chunk_size):
    chunk = df.iloc[:, range(start, min(start + chunk_size, df.shape[1]))]
    yield chunk

# COMMAND ----------

chunk_size = 10
loco_estimates = []
for label_df_chunk in chunk_columns(label_df, chunk_size):
  loco_estimates.append(glow.wgr.estimate_loco_offsets(block_df, label_df_chunk, sample_blocks, covariate_df))

# COMMAND ----------

all_traits_loco_df = pd.concat(loco_estimates, axis='columns')
all_traits_loco_df.to_csv(quantitative_y_hat_path)

end_time_wgr = time.time()
log_metadata(datetime, n_samples, n_variants, n_covariates, n_quantitative_phenotypes, method, test, library, spark_version, node_type_id, n_workers, start_time_wgr, end_time_wgr, run_metadata_delta_path)

# COMMAND ----------

adjusted_phenotypes = pd.read_csv(quantitative_y_hat_path, 
                                  dtype={'sample_id': str}, 
                                  index_col='sample_id')
adjusted_phenotypes