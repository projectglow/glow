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

# MAGIC %md
# MAGIC ##### library imports

# COMMAND ----------

import glow
spark = glow.register(spark)
import pyspark.sql.functions as fx

import json
import numpy as np
import pandas as pd
from pathlib import Path

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Generation Constants

# COMMAND ----------

#genotype matrix
n_samples = 50000
n_variants = 1000

#phenotypes
n_quantitative_phenotypes = 1

#covariates
n_covariates = 10

#wgr
variants_per_block = 1000
sample_block_count = 10

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Storage Path Constants

# COMMAND ----------

user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
dbfs_home_path_str = "dbfs:/home/{}/".format(user)
dbfs_fuse_home_path_str = "/dbfs/home/{}/".format(user)
dbfs_home_path = Path("dbfs:/home/{}/".format(user))
dbfs_fuse_home_path = Path("/dbfs/home/{}/".format(user))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Prepare input paths.
# MAGIC Glow can read variant data from common file formats like VCF, BGEN, and Plink.
# MAGIC 
# MAGIC Please checkpoint your data into Parquet or Delta Lake table before running GloWGR.
# MAGIC 
# MAGIC Note: The data in this problem is fabricated for demonstration purpose and is biologically meaningless.

# COMMAND ----------

delta_path = str(dbfs_home_path / 'genomics/data/delta/simulate_'.format(user))
base_variants_path = output_delta = delta_path + str(n_samples) + '_samples_' + str(n_variants) + '_variants_pvcf.delta'
variants_path = output_delta = delta_path + str(n_samples) + '_samples_' + str(n_variants) + '_variants_pvcf_transformed.delta'

pandas_path = str(dbfs_fuse_home_path / ('genomics/data/pandas/simulate_'.format(user) + str(n_samples) + '_samples_'))
covariates_path = pandas_path + str(n_covariates) + '_covariates.csv'
quantitative_phenotypes_path = pandas_path + str(n_quantitative_phenotypes) + '_quantitative_phenotypes.csv'
output_quantitative_offset = pandas_path + str(n_quantitative_phenotypes) + '_offset_quantitative_phenotypes.csv'

sample_blocks_path = pandas_path + '_wgr_sample_blocks.json'
block_matrix_path = delta_path + 'wgr_block_matrix.delta'
y_hat_path = pandas_path + str(n_quantitative_phenotypes) + '_wgr_y_quantitative_hat.csv'

# COMMAND ----------

base_variant_df = spark.read.format('delta').load(base_variants_path)
variant_df = spark.read.format('delta').load(variants_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Extract sample IDs from a variant DataFrame with `glow.wgr.get_sample_ids`.

# COMMAND ----------

sample_ids = glow.wgr.get_sample_ids(base_variant_df)
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

with open(sample_blocks_path, 'w') as f:
  json.dump(sample_blocks, f)
block_df.write.format('delta').mode('overwrite').save(block_matrix_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Run whole genome regression (WGR) to calculate expected phenotypes

# COMMAND ----------

block_df = spark.read.format('delta').load(block_matrix_path)
with open(sample_blocks_path, 'r') as f:
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
all_traits_loco_df.to_csv(y_hat_path)

# COMMAND ----------

test = pd.read_csv(y_hat_path)
test