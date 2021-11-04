# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Set up constants for Glow to use in GWAS workflow notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import libraries

# COMMAND ----------

import glow
spark = glow.register(spark)

import pyspark.sql.functions as fx
from pyspark.sql.types import *

import random
import string
import pandas as pd
import numpy as np
import os 
import json
import time
import pytz
from datetime import datetime
from pathlib import Path
import itertools
from collections import Counter

import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data generation constants

# COMMAND ----------

#genotype matrix
n_samples = 50000
n_variants = 1000

#partitions
n_partitions = 5 #good heuristic is 20 variants per partition at 500k samples

#allele frequency
allele_freq_cutoff = 0.05

# chromosomes for simulating pvcf
random_seed = 42
random.seed(random_seed)

#phenotypes
n_binary_phenotypes = 1
n_quantitative_phenotypes = 1
n_phenotypes = n_binary_phenotypes + n_quantitative_phenotypes

#covariates
n_quantitative_covariates = 8
n_binary_covariates = 2
n_covariates = n_quantitative_covariates + n_binary_covariates
missingness = 0.3

#wgr
variants_per_block = 1000
sample_block_count = 10

#chromosomes
contigs = ['21', '22']

# COMMAND ----------

print("variables", json.dumps({
  "n_samples": n_samples,
  "n_binary_phenotypes": n_binary_phenotypes,
  "n_quantitative_phenotypes": n_quantitative_phenotypes,
  "n_phenotypes": n_phenotypes,
  "n_binary_covariates": n_binary_covariates,
  "n_covariates": n_covariates,
  "missingness": missingness,
  "n_variants": n_variants,
  "n_partitions": n_partitions,
  "random_seed": random_seed,
  "allele_freq_cutoff": allele_freq_cutoff,
  "contigs": contigs
}
  , indent=4))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data storage path constants

# COMMAND ----------

user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
dbfs_home_path_str = "dbfs:/home/{}/".format(user)
dbfs_fuse_home_path_str = "/dbfs/home/{}/".format(user)
dbfs_home_path = Path("dbfs:/home/{}/".format(user))
dbfs_fuse_home_path = Path("/dbfs/home/{}/".format(user))

print("data storage path constants", json.dumps({
  "dbfs_fuse_home_path": str(dbfs_fuse_home_path),
  "dbfs_home_path": str(dbfs_home_path),
  "dbfs_fuse_home_path_str": dbfs_fuse_home_path_str,
  "dbfs_home_path_str": dbfs_home_path_str,
  "user": user
}
  , indent=4))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set paths

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### simulate covariates

# COMMAND ----------

dbfs_file_path = dbfs_home_path / "genomics/data/pandas/"
dbutils.fs.mkdirs(str(dbfs_file_path))

dbfs_file_fuse_path = dbfs_fuse_home_path / "genomics/data/pandas/"
simulate_file_prefix = f"simulate_{n_samples}_samples_"

output_covariates = str(dbfs_file_fuse_path / (simulate_file_prefix + f"{n_covariates}_covariates.csv"))
output_quantitative_phenotypes = str(dbfs_file_fuse_path / (simulate_file_prefix + f"{n_quantitative_phenotypes}_quantitative_phenotypes.csv"))
output_binary_phenotypes = str(dbfs_file_fuse_path / (simulate_file_prefix + f"{n_binary_phenotypes}_binary_phenotypes.csv"))

print("phenotype simulation paths", json.dumps({
  "output_covariates": output_covariates,
  "output_quantitative_phenotypes": output_quantitative_phenotypes,
  "output_binary_phenotypes": output_binary_phenotypes
}
, indent=4))


# COMMAND ----------

# MAGIC %md 
# MAGIC ##### simulate genotypes

# COMMAND ----------

vcfs_path = str(dbfs_home_path / "genomics/data/1kg-vcfs-autosomes")
vcfs_path_local = str(dbfs_fuse_home_path / "genomics/data/1kg-vcfs-autosomes")

os.environ['vcfs_path_local'] = vcfs_path_local
output_vcf_delta = str(dbfs_home_path / f'genomics/data/delta/1kg_variants_pvcf.delta')
output_simulated_delta = str(dbfs_home_path / f'genomics/data/delta/simulate_{n_samples}_samples_{n_variants}_variants_pvcf.delta')

print("genotype simulation paths", json.dumps({
  "output_vcf_delta": output_vcf_delta,
  "output_simulated_delta": output_simulated_delta
}
, indent=4))

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### delta to vcf

# COMMAND ----------

simulate_prefix = str(dbfs_home_path / f"genomics/data/delta/simulate_{n_samples}_samples_{n_variants}")

simulate_prefix_local = str(dbfs_fuse_home_path / f"genomics/data/delta/simulate_{n_samples}_samples_{n_variants}") 

output_delta = simulate_prefix + '_variants_pvcf.delta'
output_vcf = simulate_prefix + '_variants_pvcf.vcf.bgz'
output_vcf_small = simulate_prefix + '_variants_pvcf_test.vcf'

output_vcf_local = simulate_prefix_local + '_variants_pvcf_test.vcf'
os.environ["output_vcf"] = output_vcf_local

print("delta to vcf paths", json.dumps({
  "output_delta": output_delta,
  "output_vcf": output_vcf,
  "output_vcf_small": output_vcf_small,
  "output_vcf_local": output_vcf_local
}
, indent=4))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### quality control

# COMMAND ----------

output_delta_transformed = simulate_prefix + "_variants_pvcf_transformed.delta"
output_hwe_path = str(dbfs_home_path / f"genomics/data/results")
output_hwe_plot = str(dbfs_fuse_home_path / f"genomics/data/results/simulate_{n_samples}_samples_{n_variants}_hwe.png")

print("quality control paths", json.dumps({
  "output_delta": output_delta,
  "output_delta_transformed": output_delta_transformed,
  "output_hwe_path": output_hwe_path,
  "output_hwe_plot": output_hwe_plot
}
, indent=4))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### whole genome regression: quantitative

# COMMAND ----------

delta_path = str(dbfs_home_path / 'genomics/data/delta/simulate_'.format(user))
base_variants_path = delta_path + str(n_samples) + '_samples_' + str(n_variants) + '_variants_pvcf.delta'
variants_path = delta_path + str(n_samples) + '_samples_' + str(n_variants) + '_variants_pvcf_transformed.delta'
input_delta = delta_path + str(n_samples) + '_samples_' + str(n_variants) + '_variants_pvcf_transformed.delta'


pandas_path = str(dbfs_fuse_home_path / ('genomics/data/pandas/simulate_'.format(user) + str(n_samples) + '_samples_'))
covariates_path = pandas_path + str(n_covariates) + '_covariates.csv'
quantitative_phenotypes_path = pandas_path + str(n_quantitative_phenotypes) + '_quantitative_phenotypes.csv'
output_quantitative_offset = pandas_path + str(n_quantitative_phenotypes) + '_offset_quantitative_phenotypes.csv'

quantitative_sample_blocks_path = pandas_path + 'quantitative_wgr_sample_blocks.json'
quantitative_block_matrix_path = delta_path + 'quantitative_wgr_block_matrix.delta'
quantitative_y_hat_path = pandas_path + str(n_quantitative_phenotypes) + '_quantitative_wgr_y_hat.csv'


linear_gwas_results_path_confounded = delta_path + 'pvcf_linear_gwas_results_confounded.delta'
linear_gwas_results_path = delta_path + 'pvcf_linear_gwas_results.delta'

print("regression paths", json.dumps({
  "delta_path": delta_path,
  "base_variants_path": base_variants_path,
  "variants_path": variants_path,
  "pandas_path": pandas_path,
  "covariates_path": covariates_path
  }
, indent=4))

print("quantitative phenotype paths", json.dumps({
  "quantitative_phenotypes_path": quantitative_phenotypes_path,
  "output_quantitative_offset": output_quantitative_offset,
  "quantitative_sample_blocks_path": quantitative_sample_blocks_path,
  "quantitative_block_matrix_path": quantitative_block_matrix_path,
  "linear_gwas_results_path_confounded": linear_gwas_results_path_confounded,
  "linear_gwas_results_path": linear_gwas_results_path
}
, indent=4))
  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### whole genome regression: binary

# COMMAND ----------

binary_phenotypes_path = pandas_path + str(n_binary_phenotypes) + '_binary_phenotypes.csv'
output_binary_offset = pandas_path + str(n_binary_phenotypes) + '_offset_binary_phenotypes.csv'

binary_sample_blocks_path = pandas_path + 'binary_wgr_sample_blocks.json'
binary_block_matrix_path = delta_path + 'binary_wgr_block_matrix.delta'
binary_y_hat_path = pandas_path + str(n_binary_phenotypes) + '_binary_wgr_y_hat.csv'

binary_gwas_results_path = delta_path + 'simulate_pvcf_firth_gwas_results.delta'


print("binary phenotype paths", json.dumps({
  "quantitative_phenotypes_path": quantitative_phenotypes_path,
  "output_quantitative_offset": output_quantitative_offset,
  "binary_sample_blocks_path": binary_sample_blocks_path,
  "binary_block_matrix_path": binary_block_matrix_path,
  "binary_gwas_results_path": binary_gwas_results_path
}
, indent=4))