# Databricks notebook source
from pyspark.sql.types import *

# Human genome annotations in GFF3 are available at https://ftp.ncbi.nlm.nih.gov/genomes/refseq/vertebrate_mammalian/Homo_sapiens/reference/GCF_000001405.39_GRCh38.p13/
gff_path = "/databricks-datasets/genomics/gffs/GCF_000001405.39_GRCh38.p13_genomic.gff.bgz"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Read in GFF3 with inferred schema

# COMMAND ----------

# DBTITLE 0,Print inferred schema
original_gff_df = spark.read \
  .format("gff") \
  .load(gff_path) \

# COMMAND ----------

# DBTITLE 0,Read in the GFF3 with the inferred schema
display(original_gff_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Read in GFF3 with user-specified schema

# COMMAND ----------

mySchema = StructType( \
 [StructField('seqId', StringType()), 
  StructField('start', LongType()), 
  StructField('end', LongType()),
  StructField('ID', StringType()),
  StructField('Dbxref', ArrayType(StringType())),
  StructField('gene', StringType()),
  StructField('mol_type', StringType())] 
)

original_gff_df = spark.read \
  .schema(mySchema) \
  .format("gff") \
  .load(gff_path) \

display(original_gff_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Read in GFF3 with user-specified schema including original GFF3 ``attributes`` column

# COMMAND ----------

mySchema = StructType( \
 [StructField('seqId', StringType()), 
  StructField('start', LongType()), 
  StructField('end', LongType()),
  StructField('ID', StringType()),
  StructField('Dbxref', ArrayType(StringType())),
  StructField('gene', StringType()),
  StructField('mol_type', StringType()),
  StructField('attributes', StringType())] 
)

original_gff_df = spark.read \
  .schema(mySchema) \
  .format("gff") \
  .load(gff_path) \

display(original_gff_df)