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
results_path = "/dbfs/tmp/genomics"
results_dbfs_path = "dbfs:/tmp/genomics"
os.environ['results_path'] = results_path
log_path = "/dbfs/tmp/genomics/logs"
os.environ['log_path'] = log_path

# COMMAND ----------

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
# MAGIC cp Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz $results_path/reference/homo_sapiens/100/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
# MAGIC cp homo_sapiens_ancestor_GRCh38.fa.gz $results_path/reference/homo_sapiens/100/homo_sapiens_ancestor_GRCh38.fa.gz

# COMMAND ----------

display(dbutils.fs.ls(file_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### prepare test vcf

# COMMAND ----------

vcf_path = 'dbfs:/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz'
vcf_test_path = results_dbfs_path + "/test_single_node.vcf"
vcf_test_pipe_path = results_dbfs_path + "/test_pipe_annotated.vcf"
df = spark.read.format("vcf").load(vcf_path).limit(1000)
df.write.format("bigvcf").mode("overwrite").save(vcf_test_path)

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
# MAGIC pid=$$
# MAGIC hostname=`hostname`
# MAGIC current_time=$(date +"%Y%m%d") 
# MAGIC 
# MAGIC /opt/vep/src/ensembl-vep/vep \
# MAGIC -i $results_path/test_single_node.vcf \
# MAGIC --dir_cache /dbfs/tmp/genomics/reference \
# MAGIC --fasta /dbfs/tmp/genomics/reference/homo_sapiens/100/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz \
# MAGIC --plugin AncestralAllele,/dbfs/tmp/genomics/reference/homo_sapiens/100/homo_sapiens_ancestor_GRCh38.fa.gz \
# MAGIC --assembly GRCh38 \
# MAGIC --format vcf \
# MAGIC --no_stats \
# MAGIC --vcf \
# MAGIC --hgvs \
# MAGIC --cache \
# MAGIC --force_overwrite \
# MAGIC --output_file $results_path/test_single_node_annotated.vcf \
# MAGIC --warning_file $log_path/vep_stderr_warning_file_$current_time\_pid_$pid\_hostname_$hostname.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check 1000 annotated variants outputted

# COMMAND ----------

# MAGIC %sh
# MAGIC cat $results_path/test_single_node_annotated.vcf | sed '/#/d' | wc -l

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n 200  $results_path/test_single_node_annotated.vcf

# COMMAND ----------

# MAGIC %md
# MAGIC parallelize with glow pipe transformer

# COMMAND ----------

df = spark.read.format("vcf").load(vcf_test_path)
df.count()

# COMMAND ----------

scriptFile = r"""#!/bin/sh
set -e

pid=$$
hostname=`hostname`
current_time=$(date +"%Y%m%d") 

/opt/vep/src/ensembl-vep/vep \
--dir_cache /dbfs/tmp/genomics/reference \
--fasta /dbfs/tmp/genomics/reference/homo_sapiens/100/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz \
--plugin AncestralAllele,/dbfs/tmp/genomics/reference/homo_sapiens/100/homo_sapiens_ancestor_GRCh38.fa.gz \
--assembly GRCh38 \
--format vcf \
--no_stats \
--vcf \
--hgvs \
--cache \
--force_overwrite \
--output_file STDOUT \
--warning_file {0}/vep_stderr_warning_file_$current_time\_pid_$pid\_hostname_$hostname.txt
""".format(log_path)

cmd = json.dumps(["bash", "-c", scriptFile])

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
# MAGIC cat $results_path/test_pipe_annotated.vcf | sed '/#/d' | wc -l

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check single node output same as pipe transformer output

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n 200 $results_path/test_pipe_annotated.vcf

# COMMAND ----------

single_node_vcf = spark.read.format("vcf").load(results_dbfs_path + '/test_single_node_annotated.vcf')
glow_vcf = spark.read.format("vcf").load(results_dbfs_path + '/test_pipe_annotated.vcf')

# COMMAND ----------

single_node_only_df = single_node_vcf.subtract(glow_vcf)
single_node_only_count = single_node_only_df.count()

# COMMAND ----------

display(single_node_only_df)

# COMMAND ----------

try:
  assert single_node_only_count == 0
except:
  raise ValueError("single node and glow VEP output do not match")

# COMMAND ----------

glow_only_df = glow_vcf.subtract(single_node_vcf)
glow_only_count = glow_only_df.count()

# COMMAND ----------

display(glow_only_df)

# COMMAND ----------

try:
  assert glow_only_count == 0
except:
  raise ValueError("single node and glow VEP output do not match")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### clean up

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r $results_path