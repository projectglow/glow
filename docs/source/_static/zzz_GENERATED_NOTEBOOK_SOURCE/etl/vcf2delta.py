# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Write VCF files into <img width="175px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png">  
# MAGIC 
# MAGIC ### using Glow <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> 
# MAGIC 
# MAGIC Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark™ and big data workloads.
# MAGIC 
# MAGIC Delta Lake is applied in the following use cases in genomics:
# MAGIC 
# MAGIC 1. joint-genotyping of population-scale cohorts
# MAGIC   - using GATK's GenotypeGVCFs ([blog](https://databricks.com/blog/2019/06/19/accurately-building-genomic-cohorts-at-scale-with-delta-lake-and-spark-sql.html))
# MAGIC   - or custom machine learning algorithms, for example for copy-number variants
# MAGIC 2. managing variant data in data lakes ([blog](https://databricks.com/blog/2019/03/07/simplifying-genomics-pipelines-at-scale-with-databricks-delta.html))
# MAGIC   - once volumes reach thousands of VCF files 
# MAGIC 3. querying and statistical analysis of genotype data
# MAGIC   - once volumes reach billions of genotypes
# MAGIC   
# MAGIC This notebook processes chromosome 22 of the 1000 Genomes project directly from cloud storage into Delta Lake, with the following steps:
# MAGIC 
# MAGIC   1. Read in pVCF as a Spark Data Source using Glow's VCF reader
# MAGIC   2. Write out DataFrame as a Delta Lake
# MAGIC   3. Query individual genotypes
# MAGIC   4. Subset the data randomly
# MAGIC   5. Write out as VCF (to show backwards compatibility with flat file formats)

# COMMAND ----------

import glow
spark = glow.register(spark)
import pyspark.sql.functions as fx
from pyspark.sql.types import *
from random import sample

# COMMAND ----------

# MAGIC %md
# MAGIC #### set up paths

# COMMAND ----------

vcf_path = '/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz'
phenotypes_path = '/databricks-datasets/genomics/1000G/phenotypes.normalized'
reference_genome_path = "/dbfs/databricks-datasets/genomics/grch37/data/human_g1k_v37.fa"
delta_output_path = "dbfs:/home/genomics/delta/1kg-delta"
vcf_output_path = "dbfs:/home/genomics/vcf/subset.vcf"

# COMMAND ----------

# MAGIC %md
# MAGIC ####![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)  Read in pVCF as a [Spark Data Source](https://spark.apache.org/docs/latest/sql-data-sources.html) using Glow <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="30"/>

# COMMAND ----------

# MAGIC %md 
# MAGIC Note: if there are multiple files, use a wildcard (*)

# COMMAND ----------

vcf = (
  spark
  .read
  .format("vcf")
  .load(vcf_path)
)

# COMMAND ----------

display(vcf)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Print the VCF schema
# MAGIC 
# MAGIC __INFO__ fields are promoted to top level columns by default, with the prefix `INFO_`
# MAGIC 
# MAGIC __FILTER__ fields are nested in the `filters` array
# MAGIC 
# MAGIC __FORMAT__ fields are nested in the `genotypes` array

# COMMAND ----------

vcf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### split multiallelic events

# COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", False) # turn off Spark SQL whole-stage code generation for faster performance.  
split_vcf = glow.transform("split_multiallelics", vcf)

# COMMAND ----------

split_vcf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### normalize variants
# MAGIC 
# MAGIC This is an important quality control / sanity check when ingesting VCFs
# MAGIC 
# MAGIC And is always necessary after multiallelics variants are split to biallelics

# COMMAND ----------

normalized_vcf = glow.transform("normalize_variants", 
                                split_vcf,
                                reference_genome_path=reference_genome_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's count how many variants have changed after normalization

# COMMAND ----------

vcf.join(normalized_vcf, ["contigName", "start", "end", "referenceAllele", "alternateAlleles"], "left_anti").count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write out DataFrame to <img width="175px" src="https://docs.delta.io/latest/_static/delta-lake-logo.png">

# COMMAND ----------

# MAGIC %md
# MAGIC To append to existing Delta Lake use `.mode("append")`

# COMMAND ----------

(
  normalized_vcf
  .write
  .format("delta")
  .mode("overwrite")
  .save(delta_output_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Describe history of the Delta table
# MAGIC 
# MAGIC With Delta, you can append more data, and time travel back to previous versions of the table

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY delta.`{delta_output_path}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png)  Read in the Delta Lake and count the number of variants

# COMMAND ----------

delta_vcf = spark.read.format("delta").load(delta_output_path)

# COMMAND ----------

delta_vcf.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Perform point query to retrieve specific genotype
# MAGIC 
# MAGIC get genotype for a specific sample at a specific position
# MAGIC 
# MAGIC for faster queries, explode the genotypes array and Z-order on contigName and position

# COMMAND ----------

sample_id = "HG00100"
chrom = "22"
start = 16050074

# COMMAND ----------

genotype = delta_vcf.where((fx.col("contigName") == chrom) & 
                           (fx.col("start") == start)). \
                     selectExpr("contigName", "start", "filter(genotypes, g -> g.sampleId = '{0}') as genotypes".format(sample_id))

# COMMAND ----------

genotype.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select random subset of data and write back out to VCF

# COMMAND ----------

samples_list = vcf.select("genotypes"). \
                   limit(1). \
                   select(glow.subset_struct('genotypes', 'sampleId').alias('samples')). \
                   collect()[0].samples.sampleId

# COMMAND ----------

subset_samples = sample(samples_list, 100)
subset_samples_str = '\' , \''.join(subset_samples)

# COMMAND ----------

sample_filter = delta_vcf.selectExpr("*", "filter(genotypes, g -> array_contains(array('{0}'), g.sampleId)) as genotypes2".format(subset_samples_str)). \
                          drop("genotypes"). \
                          withColumnRenamed("genotypes2", "genotypes")

# COMMAND ----------

len(sample_filter.limit(1).select("genotypes.sampleId").collect()[0].sampleId)

# COMMAND ----------

# MAGIC %md
# MAGIC #### VCFs can be written as plain text or you can specify block gzip [compression](https://docs.databricks.com/applications/genomics/variant-data.html#vcf)

# COMMAND ----------

(
  sample_filter
  .write
  .format("bigvcf")
  .mode("overwrite")
  .save(vcf_output_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using the [local file API](https://docs.databricks.com/data/databricks-file-system.html#local-file-apis), we can read VCF directly from cloud storage via the shell

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n 200 /dbfs/home/genomics/vcf/subset.vcf