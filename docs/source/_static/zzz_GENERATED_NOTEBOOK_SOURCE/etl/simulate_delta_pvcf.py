# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Simulate pVCF
# MAGIC 
# MAGIC Uses the 1000 genomes to simulate a project-level VCF at a larger scale and write to delta lake
# MAGIC 
# MAGIC For now we manually define functions to handle hardy-weinberg allele frequency and multiallelic variants
# MAGIC 
# MAGIC _TODO_ use sim1000G to get realistic families to test offset correction

# COMMAND ----------

# MAGIC %md
# MAGIC ##### import libraries

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
from pathlib import Path
import itertools
from collections import Counter

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Generation Constants

# COMMAND ----------

#genotype matrix
n_samples = 500000
n_variants = 1000

# chromosomes for simulating pvcf
random_seed = 42
random.seed(random_seed)
minor_allele_frequency_cutoff = 0.005
chromosomes = ["21", "22"] #glow whole genome regression leave one chromosome out (loco) method requires at least two chromosomes

n_partitions = 5 #good heuristic is 20 variants per partition at 500k samples

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
# MAGIC ##### simulate genotypes helper functions

# COMMAND ----------

def hardy_weinberg_principle(minor_allele_frequency):
  """
  given a minor allele frequency for a biallelic variant, 
  return an array of frequencies for each genotype
  """
  p = 1-minor_allele_frequency
  q = minor_allele_frequency
  aa = p * p
  aA = 2 * p * q
  AA = q * q
  return [aa, aA, AA]

def get_allele_frequencies(minor_allele_frequency):
  """
  given a list of `minor_allele_frequency`
  add in the reference allele frequency and return a list of frequencies
  """
  ref_allele_frequency = 1 - sum(minor_allele_frequency)
  allele_frequencies = [ref_allele_frequency] +  minor_allele_frequency
  return allele_frequencies

def get_allele_frequency_combos(allele_frequencies):
  """
  given a list of allele frequencies, 
  return all combinations of frequencies
  """
  allele_frequency_product = list(itertools.product(allele_frequencies, allele_frequencies))
  allele_freq_combos = [i[0]*i[1] for i in allele_frequency_product]
  return allele_freq_combos

def get_genotype_calls_combinations(allele_frequencies):
  """
  given a list of allele frequencies, 
  return all possible genotype call combinations
  for example, if len(allele_frequencies) = 6, one combination may be [0,5]
  """
  genotypes = [i for i in range(len(allele_frequencies))]
  genotype_combinations = list(itertools.product(genotypes, genotypes)) 
  genotype_calls = [[i[0], i[1]] for i in genotype_combinations]
  return genotype_calls
  
def generate_multiallelic_frequencies(minor_allele_frequency, n_samples):
  """
  given a multiallelic variant with a list of `minor_allele_frequency`
  return an array of frequencies for each genotype for n_samples
  """ 
  allele_frequencies = get_allele_frequencies(minor_allele_frequency)
  allele_freq_combos = get_allele_frequency_combos(allele_frequencies)
  genotype_calls = get_genotype_calls_combinations(allele_frequencies)
  genotype_list = random.choices(genotype_calls, k=n_samples, weights=allele_freq_combos)
  return genotype_list

sample_id_list = [str(i) for i in range (0, n_samples)]

def simulate_genotypes(minor_allele_frequency, n_samples, sample_list=sample_id_list):
  """
  given an array that contains the minor_allele_frequency as the first element, 
  return a genotypes struct of length=n_samples that conforms to the Glow variant schema, 
  with genotypes that are in Hardy Weinberg Equilibrium
  """
  n_samples = int(n_samples)
  frequencies = hardy_weinberg_principle(minor_allele_frequency[0])
  calls = [[0,0], [0,1], [1,1]]
  if len(minor_allele_frequency) > 1:
    genotype_list = generate_multiallelic_frequencies(minor_allele_frequency, n_samples)
  else:
    genotype_list = random.choices(calls, k=n_samples, weights=frequencies)
  new_lst = [list(x) for x in zip(sample_id_list, genotype_list)]
  genotypes = [{"sampleId":x, "calls": y} for x, y in new_lst]
  return genotypes

simulate_genotypes_udf = udf(simulate_genotypes, ArrayType(StructType([
              StructField("sampleId", StringType(), True),
              StructField("calls", ArrayType(IntegerType(), True))
              ])))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### set paths

# COMMAND ----------

vcfs_path = str(dbfs_home_path / "genomics/data/1kg-vcfs-autosomes")
vcfs_path_local = str(dbfs_fuse_home_path / "genomics/data/1kg-vcfs-autosomes")

os.environ['vcfs_path_local'] = vcfs_path_local
output_vcf_delta = str(dbfs_home_path / f'genomics/data/delta/1kg_variants_pvcf.delta')
output_simulated_delta = str(dbfs_home_path / f'genomics/data/delta/simulate_{n_samples}_samples_{n_variants}_variants_pvcf.delta')
vcfs_path, vcfs_path_local, output_vcf_delta, output_simulated_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ##### download 1000G data for chrom 21 and 22

# COMMAND ----------

# MAGIC %sh
# MAGIC declare -a chroms=("21" "22")
# MAGIC 
# MAGIC for i in "${chroms[@]}"; do wget ftp://hgdownload.cse.ucsc.edu/gbdb/hg19/1000Genomes/phase3/ALL.chr$i.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz; done
# MAGIC 
# MAGIC mkdir -p $vcfs_path_local
# MAGIC 
# MAGIC cp ALL*.genotypes.vcf.gz $vcfs_path_local

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read 1000 Genomes VCF

# COMMAND ----------

vcf = spark.read.format("vcf").load(vcfs_path) \
                              .drop("genotypes") \
                              .where(fx.col("INFO_AF")[0] >= minor_allele_frequency_cutoff)
total_variants = vcf.count()
fraction = n_variants / total_variants

# COMMAND ----------

# MAGIC %md
# MAGIC ##### checkpoint to delta

# COMMAND ----------

vcf.write.mode("overwrite").format("delta").save(output_vcf_delta)

# COMMAND ----------

vcf = spark.read.format("delta").load(output_vcf_delta)

# COMMAND ----------

display(vcf)

# COMMAND ----------

simulated_vcf = vcf.sample(withReplacement=False, fraction=fraction) \
                   .repartition(n_partitions) \
                   .withColumn("genotypes", simulate_genotypes_udf(fx.col("INFO_AF"), 
                                                                   fx.lit(n_samples)))

# COMMAND ----------

simulated_vcf.count()

# COMMAND ----------

display(simulated_vcf.drop("genotypes"))

# COMMAND ----------

simulated_vcf.write.mode("overwrite").format("delta").save(output_simulated_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check output delta table

# COMMAND ----------

delta_vcf = spark.read.format("delta").load(output_simulated_delta).drop("genotypes")

# COMMAND ----------

display(delta_vcf)

# COMMAND ----------

display(delta_vcf.groupBy("contigName").count())