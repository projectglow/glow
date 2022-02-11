# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Set up constants for Hail to use in GWAS workflow notebooks

# COMMAND ----------

# MAGIC %run ./0_setup_constants_glow

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import libraries

# COMMAND ----------

import hail as hl
hl.init(sc, idempotent=True, quiet=True, skip_logging_configuration=True)

import glow
spark = glow.register(spark)
from glow.hail import functions

import pandas as pd
import pyspark.sql.functions as fx
from pyspark.sql.types import *

from pathlib import Path

import time
import pytz
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Set Constants

# COMMAND ----------

#partitions
n_partitions_hail = 5 #match to glow, however this may not be optimal for hail so can be configured

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set paths

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data storage path constants

# COMMAND ----------

dbfs_home_path_str = "dbfs:/home/{}/".format(user)
dbfs_home_path = Path("dbfs:/home/{}/".format(user))

print("data storage path constants", json.dumps({
  "dbfs_home_path": str(dbfs_home_path),
  "dbfs_home_path_str": dbfs_home_path_str,
  "user": user
}
  , indent=4))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Convert VCF file to hail matrix table

# COMMAND ----------

simulate_hail_prefix = str(dbfs_home_path / ('genomics/data/hail/simulate_' + str(n_samples) + '_samples_' + str(n_variants)))

input_vcf = output_vcf #from delta to vcf notebook
hail_matrix_table_outpath = simulate_hail_prefix + '_variants.mt'

print("delta to vcf paths", json.dumps({
  "input_vcf": input_vcf,
  "hail_matrix_table_outpath": hail_matrix_table_outpath
}
, indent=4))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Hail matrix table to glow VCF schema

# COMMAND ----------

delta_table_outpath = simulate_prefix + '_variants_from_hail.delta'
delta_table_from_vcf_path = simulate_prefix + '_variants_pvcf.delta'


print("delta to vcf paths", json.dumps({
  "hail_matrix_table_path": hail_matrix_table_outpath,
  "delta_table_outpath": delta_table_outpath,
  "delta_table_from_vcf_path": delta_table_from_vcf_path
}
, indent=4))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Linear regression

# COMMAND ----------

hail_matrix_table = hail_matrix_table_outpath
quantitative_gwas_results_chk = 'dbfs:/home/{}/genomics/data/hail/simulate_pvcf_linreg_gwas_results_hail.chk'.format(user)
gwas_results_path = str(dbfs_home_path / "genomics/data/delta/gwas_results_hail_glow.delta")
run_metadata_delta_path = str(dbfs_home_path / "genomics/data/delta/gwas_runs_info_hail_glow.delta")
path = '/dbfs/home/{}/genomics/data/pandas/simulate_'.format(user) + str(n_samples) + '_samples_'
covariates_path = path + str(n_covariates) + '_covariates.csv'
quantitative_phenotypes_path = path + str(n_quantitative_phenotypes) + '_quantitative_phenotypes.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Logistic regression

# COMMAND ----------

gwas_results_path_glow = 'dbfs:/home/{}/genomics/data/delta/simulate_pvcf_linreg_gwas_results_glow.delta'.format(user)
binary_gwas_results_chk = 'dbfs:/home/{}/genomics/data/hail/simulate_pvcf_firth_gwas_results_hail.chk'.format(user)
binary_phenotypes_path = path + str(n_binary_phenotypes) + '_binary_phenotypes.csv'