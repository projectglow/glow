# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> + <img src="https://www.regeneron.com/sites/all/themes/regeneron_corporate/images/science/logo-rgc-color.png" alt="logo" width="240"/>
# MAGIC 
# MAGIC ### <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> GloWGR: whole genome regression
# MAGIC 
# MAGIC ### Binary phenotypes

# COMMAND ----------

# MAGIC %pip install bioinfokit

# COMMAND ----------

import glow
from glow import *
from glow.wgr.functions import *
from glow.wgr.linear_model import *

import numpy as np
import pandas as pd
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from matplotlib import pyplot as plt
from bioinfokit import visuz

# COMMAND ----------

dbutils.widgets.text('variants_per_block', '1000')
dbutils.widgets.text('sample_block_count', '10')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 0: Prepare input paths.
# MAGIC Note: The data in this problem is fabricated for demonstration purpose and is bilogically meaningless.

# COMMAND ----------

variants_path = 'dbfs:/databricks-datasets/genomics/gwas/hapgen-variants.delta'
binary_phenotypes_path = '/dbfs/databricks-datasets/genomics/gwas/Ysim_binary_test_simulation.csv'
covariates_path = '/dbfs/databricks-datasets/genomics/gwas/Covs_test_simulation.csv'

y_hat_path = '/dbfs/tmp/wgr_y_hat.csv'
binary_gwas_results_path = '/dbfs/tmp/binary_wgr_gwas_results.delta'

# COMMAND ----------

variants_per_block = int(dbutils.widgets.get('variants_per_block'))
sample_block_count = int(dbutils.widgets.get('sample_block_count'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Load variants
# MAGIC To prepare the data for analysis, we perform the following transformations:
# MAGIC - Split multiallelic variants with the ``split_multiallelics`` transformer.
# MAGIC - Calculate the number of alternate alleles for biallelic variants with `genotype_states`.
# MAGIC - Replace any missing values with the mean of the non-missing values using `mean_substitute`.
# MAGIC - Filter out all homozygous SNPs.

# COMMAND ----------

base_variant_df = spark.read.format('delta').load(variants_path)
variant_df = glow.transform('split_multiallelics', base_variant_df) \
  .withColumn('values', mean_substitute(genotype_states(col('genotypes')))) \
  .filter(size(array_distinct('values')) > 1) \
  .alias('variant_df')

# COMMAND ----------

display(variant_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Extract sample IDs from a variant DataFrame with `glow.wgr.functions.get_sample_ids`.

# COMMAND ----------

sample_ids = get_sample_ids(variant_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Create the beginning block genotype matrix and sample block ID mapping with `glow.wgr.functions.block_variants_and_samples`.

# COMMAND ----------

block_df, sample_blocks = block_variants_and_samples(variant_df, 
                                                     sample_ids, 
                                                     variants_per_block, 
                                                     sample_block_count)

# COMMAND ----------

display(block_df.limit(20))

# COMMAND ----------

sample_blocks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Read simulated phenotypes and covariates data from cloud storage (S3 or ADLS) as though it were on the local filesystem via the Databricks file system (DBFS). The phenotypes and covariates should have no missing values.

# COMMAND ----------

label_df = pd.read_csv(binary_phenotypes_path, index_col='sample_id')[['Trait_1', 'Trait_2']]

# COMMAND ----------

label_df['Trait_1'].sum()

# COMMAND ----------

covariate_df = pd.read_csv(covariates_path, index_col='sample_id')
covariate_df = covariate_df.fillna(covariate_df.mean())
covariate_df = (covariate_df - covariate_df.mean()) / covariate_df.std()
covariate_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Run whole genome regression (WGR) to calculate expected phenotypes

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Perform dimensionality reduction on the block genotype matrix with ``glow.wgr.linear_model.RidgeReducer``.

# COMMAND ----------

stack = RidgeReducer()
reduced_block_df = stack.fit_transform(block_df, 
                             label_df, 
                             sample_blocks, 
                             covariate_df)
reduced_block_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Calculate expected phenotypes per label and per sample with ``glow.wgr.linear_model.LogisticRegression`` under the leave-one-chromosome-out (LOCO) scheme.

# COMMAND ----------

estimator = LogisticRegression()
model_df, cv_df = estimator.fit(reduced_block_df, 
                                label_df, 
                                sample_blocks, 
                                covariate_df)
model_df.cache()
cv_df.cache()

# COMMAND ----------

y_hat_df = estimator.transform_loco(
  reduced_block_df,
  label_df,
  sample_blocks,
  model_df,
  cv_df)
y_hat_df

# COMMAND ----------

y_hat_df.to_csv(y_hat_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: Run logistic regression GWAS with adjusted phenotypes
# MAGIC 
# MAGIC Use the WGR-predicted phenotypes as offset during GWAS using logistics regression.

# COMMAND ----------

# Convert the pandas dataframe into a Spark DataFrame
reshaped_phenotypes = reshape_for_gwas(spark, label_df).alias('reshaped_phenotypes')
offset = reshape_for_gwas(spark, y_hat_df).alias('offset')


# COMMAND ----------

wgr_gwas = variant_df.join(reshaped_phenotypes).join(offset, ['contigName']).select(
  'contigName',
  'start',
  'names',
  'reshaped_phenotypes.label',
  expand_struct(logistic_regression_gwas( 
    variant_df.values,
    reshaped_phenotypes.values,
    lit(covariate_df.to_numpy()),
    'lrt', # use 'firth' for Firth logistic regression 
    offset.values
  )))\
.filter('beta!="NaN"')
  
display(wgr_gwas.limit(20))


# COMMAND ----------

wgr_gwas.write.format('delta').mode('overwrite').save(binary_gwas_results_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4: Visualize GWAS results.

# COMMAND ----------

pdf = spark.read.format('delta').load(binary_gwas_results_path).toPandas()

# COMMAND ----------

visuz.marker.mhat(pdf.loc[pdf.label == 'Trait_2', :], chr='contigName', pv='pValue', show=True, gwas_sign_line=True)

# COMMAND ----------

values = pdf.loc[pdf.label == 'Trait_2', 'pValue']
fig, ax = plt.subplots()
ax.set_xlim((0, 6))
ax.set_ylim((0, 6))
expected = -np.log10(np.linspace(10, 0, len(values), endpoint=False))
ax.scatter(expected, np.sort(-np.log10(values)))
ax.plot(ax.get_xlim(), ax.get_ylim(), ls='--')
ax.set_xlabel('Expected -log10(p)')
ax.set_ylabel('Observed -log10(p)')