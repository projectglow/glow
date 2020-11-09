# Databricks notebook source
# DBTITLE 1,Define path variables
import glow
glow.register(spark)
vcf_path = '/databricks-datasets/genomics/variant-splitting/01_IN_altered_multiallelic.vcf'

# COMMAND ----------

# DBTITLE 1,Load a VCF into a DataFrame
original_variants_df = spark.read\
  .format("vcf")\
  .option("includeSampleIds", False)\
  .option("flattenInfoFields", True)\
  .load(vcf_path)

# COMMAND ----------

# DBTITLE 1,Display
display(original_variants_df)

# COMMAND ----------

# DBTITLE 1,Split multi-allelic variants
spark.conf.set("spark.sql.codegen.wholeStage", False) # turn off Spark SQL whole-stage code generation for faster performance.  

split_variants_df = glow.transform(\
  "split_multiallelics",\
   original_variants_df\
)

display(split_variants_df)