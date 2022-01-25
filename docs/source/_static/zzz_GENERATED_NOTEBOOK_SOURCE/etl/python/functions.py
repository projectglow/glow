# Databricks notebook source
# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../../0_setup_constants_glow

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