# Databricks notebook source
# MAGIC %md
# MAGIC # Using Pandas UDFs with Genomic Data
# MAGIC [Pandas UDFs](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html) provide a fast way to use Python libraries with Apache Spark™ DataFrames. 
# MAGIC 
# MAGIC In this notebook, we demonstrate how to use this functionality to apply the linear mixed model implementation from the popular [statsmodels](https://www.statsmodels.org/stable/mixed_linear.html?highlight=linear%20mixed%20model) library to a DataFrame of genomic data from the [1000 genomes project](https://www.internationalgenome.org/). The population as provided by 1000 genomes sample annotations is used as the grouping variable.

# COMMAND ----------

import pyspark.sql.functions as fx
import pandas as pd
import statsmodels.api as sm
import numpy as np
from pyspark.sql.types import DoubleType
import glow
spark = glow.register(spark)

# COMMAND ----------

# DBTITLE 1,Get the population annotation for samples in 1000genomes phase 3
pop = pd.read_csv('https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/integrated_call_samples_v3.20200731.ALL.ped', sep='\t')
pop.head(5)

# COMMAND ----------

groups = pop[pop['phase 3 genotypes'] == 1]['Population']
groups_bc = sc.broadcast(groups)

# COMMAND ----------

# DBTITLE 1,Define a function to fit one LMM
def run_one_lmm(genotypes, phenotypes, groups):
  try:
    intercept = np.ones(genotypes.size)
    genotypes = genotypes.copy()
    x = np.stack([intercept, genotypes], axis = 1)
    # Return p-value for genotype coefficient
    return sm.MixedLM(phenotypes, x, groups).fit().pvalues[1]
  except np.linalg.LinAlgError:
    # Could not fit model, return NaN
    return float('nan')

# COMMAND ----------

# DBTITLE 1,Define a function to fit an LMM from Pandas series of phenotypes and phenotypes
def lmm(genotypes_series, phenotypes_series): 
  groups = groups_bc.value
  df = pd.concat([genotypes_series, phenotypes_series], axis = 1)
  return pd.Series([run_one_lmm(r[0], r[1], groups) for r in df.values])

lmm_udf = fx.pandas_udf(lmm, returnType=DoubleType())

# COMMAND ----------

# DBTITLE 1,Prepare the input DataFrame
"""
Read in 1000genomes phase 3 chr 22 and split multiallelic sites to biallelic.

Add the phenotypes by cross joining with the genomic DataFrame.

The input to the lmm is the genotype represented as the number of alt alleles (0, 1, or 2).
In this example, we remove all sites where some samples are missing (as represented by -1).
"""

df = glow.transform( \
         "split_multiallelics", \
         spark.read.format("vcf").load("/databricks-datasets/genomics/1kg-vcfs/*chr22*.vcf.gz") \
     ) \
     .crossJoin(spark.read.format("parquet").load("/databricks-datasets/genomics/1000G/phenotypes.normalized/")) \
     .withColumn('genotype_states', fx.expr("genotype_states(genotypes)")) \
     .where(~fx.array_contains(fx.col('genotype_states'), -1))

# COMMAND ----------

# DBTITLE 1,Run the UDF and display results
by_pvalue = df.limit(1000).select("contigName", "start", "names", lmm_udf(df['genotype_states'], df['values']).alias("pValue"))\
  .na.drop(subset=["pValue"])\
  .orderBy("pValue", ascending=True)

display(by_pvalue)