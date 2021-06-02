# Databricks notebook source
# MAGIC %md
# MAGIC ##### Download VEP prerequisite files

# COMMAND ----------

import json
import filecmp
import os
import pyspark.sql.functions as fx
import glow
glow.register(spark)

# COMMAND ----------

file_path = "dbfs:/tmp/genomics/reference/homo_sapiens/100/"
dbutils.fs.mkdirs(file_path)

# COMMAND ----------

# MAGIC %sh
# MAGIC wget ftp://ftp.ensembl.org/pub/release-100/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
# MAGIC wget ftp://ftp.ensembl.org/pub/current_fasta/ancestral_alleles/homo_sapiens_ancestor_GRCh38.tar.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC gzip -d Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
# MAGIC bgzip Homo_sapiens.GRCh38.dna.primary_assembly.fa
# MAGIC tar xfz homo_sapiens_ancestor_GRCh38.tar.gz
# MAGIC cat homo_sapiens_ancestor_GRCh38/*.fa | bgzip -c > homo_sapiens_ancestor_GRCh38.fa.gz

# COMMAND ----------

# MAGIC %md
# MAGIC ##### copy files to dbfs for access from cloud storage

# COMMAND ----------

# MAGIC %sh
# MAGIC cp Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz /dbfs/tmp/genomics/reference/homo_sapiens/100/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
# MAGIC cp homo_sapiens_ancestor_GRCh38.fa.gz /dbfs/tmp/genomics/reference/homo_sapiens/100/homo_sapiens_ancestor_GRCh38.fa.gz

# COMMAND ----------

display(dbutils.fs.ls(file_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### prepare test vcf

# COMMAND ----------

vcf_path = 'dbfs:/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz'
vcf_test_path = "dbfs:/tmp/genomics/test_single_node.vcf"
vcf_test_pipe_path = "dbfs:/tmp/genomics/test_pipe_annotated.vcf"
df = spark.read.format("vcf").load(vcf_path).limit(1000)
df.write.format("bigvcf").save(vcf_test_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check vep installed

# COMMAND ----------

# MAGIC %sh
# MAGIC /opt/vep/src/ensembl-vep/vep

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run VEP on single node from driver

# COMMAND ----------

# MAGIC %sh
# MAGIC /opt/vep/src/ensembl-vep/vep -i /dbfs/tmp/genomics/test_single_node.vcf --dir_cache /dbfs/tmp/genomics/reference --fasta /dbfs/tmp/genomics/reference/homo_sapiens/100/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz --plugin AncestralAllele,/dbfs/tmp/genomics/reference/homo_sapiens/100/homo_sapiens_ancestor_GRCh38.fa.gz --assembly GRCh38 --format vcf --output_file /dbfs/tmp/genomics/test_single_node_annotated.vcf --no_stats --vcf --hgvs --cache --force_overwrite

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check 1000 annotated variants outputted

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/tmp/genomics/test_single_node_annotated.vcf | sed '/#/d' | wc -l

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n 200  /dbfs/tmp/genomics/test_single_node_annotated.vcf

# COMMAND ----------

# MAGIC %md
# MAGIC parallelize with glow pipe transformer

# COMMAND ----------

cmd = json.dumps([
  "/opt/vep/src/ensembl-vep/vep",
  "--dir_cache", '/dbfs/tmp/genomics/reference',
  "--fasta", "/dbfs/tmp/genomics/reference/homo_sapiens/100/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz",
  "--plugin", "AncestralAllele,/dbfs/tmp/genomics/reference/homo_sapiens/100/homo_sapiens_ancestor_GRCh38.fa.gz",
  "--assembly", "GRCh38",
  "--format", "vcf",
  "--output_file", "STDOUT",
  "--no_stats",
  "--cache",
  "--vcf",
  "--hgvs"])

# COMMAND ----------

df = spark.read.format("vcf").load(vcf_test_path)
df.count()

# COMMAND ----------

output_df = glow.transform("pipe", df, 
                           cmd=cmd, 
                           input_formatter='vcf', 
                           in_vcf_header='infer', 
                           output_formatter='vcf'
                          )

# COMMAND ----------

output_df.write. \
          format("bigvcf"). \
          mode("overwrite"). \
          save(vcf_test_pipe_path)

# COMMAND ----------

output_df.count()

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/tmp/genomics/test_pipe_annotated.vcf | sed '/#/d' | wc -l

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check single node output same as pipe transformer output

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n 200 /dbfs/tmp/genomics/test_pipe_annotated.vcf

# COMMAND ----------

# MAGIC %sh
# MAGIC grep -v "#" /dbfs/tmp/genomics/test_pipe_annotated.vcf | sort -k1,1 -k2,2n -k3 -k4 -k5 > /dbfs/tmp/genomics/test_pipe_annotated_minus_header.vcf
# MAGIC grep -v "#" /dbfs/tmp/genomics/test_single_node_annotated.vcf | sort -k1,1 -k2,2n -k3 -k4 -k5 > /dbfs/tmp/genomics/test_single_node_annotated_minus_header.vcf

# COMMAND ----------

f1 = '/dbfs/tmp/genomics/test_single_node_annotated_minus_header.vcf'
f2 = '/dbfs/tmp/genomics/test_pipe_annotated_minus_header.vcf'
if not filecmp.cmp(f1, f2):
                raise ValueError(
                    "VCF: {f1} does not match VCF: {f2}.".format(f1=f1, f2=f2)
                ) 

# COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/genomics/", recurse=True)

# COMMAND ----------


