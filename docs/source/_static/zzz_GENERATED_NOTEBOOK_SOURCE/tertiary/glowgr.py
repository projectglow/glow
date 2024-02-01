# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> + <img src="https://www.regeneron.com/Content/images/science/regenron.png" alt="logo" width="240"/>
# MAGIC
# MAGIC ### <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> GloWGR: whole genome regression

# COMMAND ----------

import glow

import json
import numpy as np
import pandas as pd
import pyspark.sql.functions as fx

spark = glow.register(spark)

# COMMAND ----------

dbutils.widgets.text('variants_per_block', '1000')
dbutils.widgets.text('sample_block_count', '10')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 0: Prepare input paths

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Glow can read variant data from common file formats like VCF, BGEN, and Plink. However, for best performance we encourage you to load your data in a Delta Lake table before running GloWGR.

# COMMAND ----------

variants_path = 'dbfs:/databricks-datasets/genomics/gwas/hapgen-variants.delta'
phenotypes_path = '/dbfs/databricks-datasets/genomics/gwas/Ysim_test_simulation.csv'
covariates_path = '/dbfs/databricks-datasets/genomics/gwas/Covs_test_simulation.csv'

block_matrix_path = 'dbfs:/tmp/wgr_block_matrix.delta'
sample_blocks_path = '/dbfs/tmp/sample_blocks.json'
y_hat_path = '/dbfs/tmp/wgr_y_hat.csv'
gwas_results_path = '/dbfs/tmp/wgr_gwas_results.delta'

# COMMAND ----------

variants_per_block = int(dbutils.widgets.get('variants_per_block'))
sample_block_count = int(dbutils.widgets.get('sample_block_count'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Load variants

# COMMAND ----------

base_variant_df = spark.read.format('delta').load(variants_path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Extract sample IDs from a variant DataFrame with `glow.wgr.get_sample_ids`.

# COMMAND ----------

sample_ids = glow.wgr.get_sample_ids(base_variant_df)

# COMMAND ----------

# MAGIC %md
# MAGIC To prepare the data for analysis, we perform the following transformations:
# MAGIC - Split multiallelic variants with the ``split_multiallelics`` transformer.
# MAGIC - Calculate the number of alternate alleles for biallelic variants with `genotype_states`.
# MAGIC - Replace any missing values with the mean of the non-missing values using `mean_substitute`.
# MAGIC - Filter out all homozygous SNPs.

# COMMAND ----------

variant_df = (glow.transform('split_multiallelics', base_variant_df)
  .withColumn('values', glow.mean_substitute(glow.genotype_states('genotypes')))
  .filter(fx.size(fx.array_distinct('values')) > 1))

# COMMAND ----------

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

# MAGIC %md
# MAGIC Read back the block matrix and sample blocks.

# COMMAND ----------

block_df = spark.read.format('delta').load(block_matrix_path)
with open(sample_blocks_path, 'r') as f:
  sample_blocks = json.load(f)

# COMMAND ----------

sample_blocks

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Read simulated phenotypes and covariates data from cloud storage (S3 or ADLS) as though it were on the local filesystem via the Databricks file system (DBFS).

# COMMAND ----------

label_df = pd.read_csv(phenotypes_path, index_col='sample_id')
label_df

# COMMAND ----------

covariate_df = pd.read_csv(covariates_path, index_col='sample_id')
covariate_df

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

# MAGIC %md
# MAGIC Merge LOCO prediction DataFrames and save to a CSV

# COMMAND ----------

all_traits_loco_df = pd.concat(loco_estimates, axis='columns')
all_traits_loco_df.to_csv(y_hat_path)