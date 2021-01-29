# Databricks notebook source
# DBTITLE 1,Setup
# MAGIC %md 
# MAGIC To use variant normalizer, a copy of the reference genome `.fa/.fasta` file (along with its `.fai` file) must be downloaded to each node of the cluster. 
# MAGIC 
# MAGIC Here, we assume the reference genome is downloaded to the following path: `/mnt/dbnucleus/dbgenomics/grch38/data/GRCh38_full_analysis_set_plus_decoy_hla.fa`
# MAGIC 
# MAGIC If you are using a Databricks cluster with [Databricks Runtime for Genomics](https://docs.databricks.com/applications/genomics/index.html), this can be done by setting the [environment variable](https://docs.databricks.com/user-guide/clusters/spark-config.html#environment-variables) ``refGenomeId=grch38`` for your cluster.

# COMMAND ----------

# DBTITLE 1,Define path variables
import glow
glow.register(spark)
ref_genome_path = '/mnt/dbnucleus/dbgenomics/grch38/data/GRCh38_full_analysis_set_plus_decoy_hla.fa'
vcf_path = '/databricks-datasets/genomics/variant-normalization/test_left_align_hg38.vcf'

# COMMAND ----------

# DBTITLE 1,Load a VCF into a DataFrame
original_variants_df = spark.read\
  .format("vcf")\
  .option("includeSampleIds", False)\
  .load(vcf_path)

# COMMAND ----------

# DBTITLE 1,Display
display(original_variants_df)

# COMMAND ----------

# DBTITLE 1,Normalize variants using normalize_variants transformer with column replacement
normalized_variants_df = glow.transform(\
  "normalize_variants",\
  original_variants_df,\
  reference_genome_path=ref_genome_path
)

display(normalized_variants_df)

# COMMAND ----------

# DBTITLE 1,Normalize variants using normalize_variants transformer without column replacement
normalized_variants_df = glow.transform(\
  "normalize_variants",\
  original_variants_df,\
  reference_genome_path=ref_genome_path,
  replace_columns="False"
)

display(normalized_variants_df)

# COMMAND ----------

# DBTITLE 1,Normalize variants using normalize_variant function
from glow.functions import *

normalized_variants_df = original_variants_df.select("*", normalize_variant("contigName", "start", "end", "referenceAllele", "alternateAlleles", ref_genome_path).alias("normalizationResult"))

display(normalized_variants_df)