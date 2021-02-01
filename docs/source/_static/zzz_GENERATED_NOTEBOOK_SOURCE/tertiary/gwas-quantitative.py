# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> + <img src="https://www.regeneron.com/sites/all/themes/regeneron_corporate/images/science/logo-rgc-color.png" alt="logo" width="240"/>
# MAGIC 
# MAGIC ### <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> GloWGR: genome-wide association study
# MAGIC 
# MAGIC ### Linear regression
# MAGIC 
# MAGIC This notebook shows how to use the `linear_regression` function in Glow to perform a genome-wide association study for quantitative traits. We incorporate the whole genome regression predictions to control for population structure and relatedness.

# COMMAND ----------

# MAGIC %pip install bioinfokit==0.8.5

# COMMAND ----------

import glow

import json
import numpy as np
import pandas as pd
import pyspark.sql.functions as fx

from matplotlib import pyplot as plt
from bioinfokit import visuz

spark = glow.register(spark)

# COMMAND ----------

variants_path = 'dbfs:/databricks-datasets/genomics/gwas/hapgen-variants.delta'
phenotypes_path = '/dbfs/databricks-datasets/genomics/gwas/Ysim_test_simulation.csv'
covariates_path = '/dbfs/databricks-datasets/genomics/gwas/Covs_test_simulation.csv'

# y_hat_path contains the whole genome regression predictions
y_hat_path = '/dbfs/tmp/wgr_y_hat.csv'
gwas_results_path = 'dbfs:/tmp/wgr_lin_reg_results.delta'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Load input data

# COMMAND ----------

base_variant_df = spark.read.format('delta').load(variants_path)

# COMMAND ----------

variant_df = (glow.transform('split_multiallelics', base_variant_df)
  .withColumn('values', glow.mean_substitute(glow.genotype_states('genotypes')))
  .filter(fx.size(fx.array_distinct('values')) > 1))

# COMMAND ----------

phenotype_df = pd.read_csv(phenotypes_path, index_col='sample_id')
phenotype_df

# COMMAND ----------

covariate_df = pd.read_csv(covariates_path, index_col='sample_id')
covariate_df

# COMMAND ----------

offset_df = pd.read_csv(y_hat_path, dtype={'contigName': str}).set_index(['sample_id', 'contigName'])
offset_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Run a genome-wide association study
# MAGIC 
# MAGIC Note that Glow can run multiple contigs in a single command. However, for large cohorts, it's more performant to run one contig at a time.

# COMMAND ----------

contigs = ['21', '22']

# COMMAND ----------

for contig in contigs:
  results = glow.gwas.linear_regression(
    variant_df.where(fx.col('contigName') == contig),
    phenotype_df,
    covariate_df,
    offset_df,
    values_column='values',
    contigs=[contig])
  
  # Write the results to a Delta Lake table partitioned by contigName
  results.write.format('delta').partitionBy('contigName').mode('append').save(gwas_results_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Inspect results

# COMMAND ----------

results_df = spark.read.format('delta').load(gwas_results_path)
display(results_df)

# COMMAND ----------

pdf = results_df.toPandas()

# COMMAND ----------

visuz.marker.mhat(pdf.loc[pdf.phenotype == 'Trait_2', :], chr='contigName', pv='pvalue', show=True, gwas_sign_line=True)