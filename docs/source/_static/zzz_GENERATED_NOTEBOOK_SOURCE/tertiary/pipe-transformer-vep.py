# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Run VEP with the glow pipe transformer

# COMMAND ----------

# MAGIC %md
# MAGIC #### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %md
# MAGIC #### download VEP prerequisite files

# COMMAND ----------

# MAGIC %sh
# MAGIC wget ftp://ftp.ensembl.org/pub/release-100/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
# MAGIC wget ftp://ftp.ensembl.org/pub/current_fasta/ancestral_alleles/homo_sapiens_ancestor_GRCh38.tar.gz
# MAGIC wget ftp://ftp.ensembl.org/pub/release-100/variation/indexed_vep_cache/homo_sapiens_vep_100_GRCh38.tar.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC gzip -d Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
# MAGIC bgzip Homo_sapiens.GRCh38.dna.primary_assembly.fa
# MAGIC 
# MAGIC tar xfz homo_sapiens_ancestor_GRCh38.tar.gz
# MAGIC cat homo_sapiens_ancestor_GRCh38/*.fa | bgzip -c > homo_sapiens_ancestor_GRCh38.fa.gz
# MAGIC 
# MAGIC tar xzf homo_sapiens_vep_100_GRCh38.tar.gz

# COMMAND ----------

# MAGIC %md
# MAGIC ##### copy files to dbfs for access from cloud storage

# COMMAND ----------

# MAGIC %sh
# MAGIC sleep 10
# MAGIC cp Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz $dircache_file_path_local/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz
# MAGIC cp homo_sapiens_ancestor_GRCh38.fa.gz $dircache_file_path_local/homo_sapiens_ancestor_GRCh38.fa.gz
# MAGIC cp -R ./homo_sapiens $dircache_file_path_local

# COMMAND ----------

# MAGIC %md
# MAGIC #### prepare test data
# MAGIC 
# MAGIC Select only the first sample so the full genotypes array is not piped through VEP

# COMMAND ----------

df = spark.read.format("delta").load(input_delta_vep) \
                               .withColumn("genotypes", fx.array(fx.col("genotypes")[0])) \
                               .write \
                               .mode("overwrite") \
                               .format("delta") \
                               .save(input_delta_vep_tmp)

# COMMAND ----------

df = spark.read.format("delta").load(input_delta_vep_tmp)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### write to vcf to test single node implementation

# COMMAND ----------

df.sort("contigName", "start", "end", "referenceAllele") \
  .write \
  .format("bigvcf") \
  .mode("overwrite") \
  .save(output_vcf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check vep installed

# COMMAND ----------

# MAGIC %sh
# MAGIC /opt/vep/src/ensembl-vep/vep

# COMMAND ----------

# MAGIC %md
# MAGIC ##### test VEP on single node

# COMMAND ----------

# MAGIC %sh
# MAGIC /opt/vep/src/ensembl-vep/vep -i $output_vcf_local --dir_cache $dircache_file_path_local --fasta $dircache_file_path_local/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz --plugin AncestralAllele,$dircache_file_path_local/homo_sapiens_ancestor_GRCh38.fa.gz --assembly GRCh38 --format vcf --output_file $annotated_vcf_local --no_stats --vcf --hgvs --cache --offline --force_overwrite

# COMMAND ----------

# MAGIC %sh
# MAGIC head -n 200 $annotated_vcf_local

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### parallelize with glow pipe transformer

# COMMAND ----------

scriptFile = r"""#!/bin/sh
set -e

pid=$$
hostname=`hostname`
current_time=$(date +"%Y%m%d") 

/opt/vep/src/ensembl-vep/vep \
--dir_cache {0} \
--fasta {0}/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz \
--plugin AncestralAllele,{0}/homo_sapiens_ancestor_GRCh38.fa.gz \
--assembly GRCh38 \
--format vcf \
--no_stats \
--vcf \
--hgvs \
--cache \
--force_overwrite \
--output_file STDOUT \
--warning_file {1}/vep_stderr_warning_file_$current_time\_pid_$pid\_hostname_$hostname.txt
""".format(dircache_file_path_local, log_path)

cmd = json.dumps(["bash", "-c", scriptFile])

# COMMAND ----------

output_df = glow.transform("pipe", 
                           df, 
                           cmd=cmd, 
                           input_formatter='vcf', 
                           in_vcf_header='infer', 
                           output_formatter='vcf'
                          )

# COMMAND ----------

output_df.write. \
          format("bigvcf"). \
          mode("overwrite"). \
          save(output_pipe_vcf)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### check results

# COMMAND ----------

annotated_vcf_pipe = spark.read \
                          .format("vcf") \
                          .load(output_pipe_vcf)

# COMMAND ----------

display(annotated_vcf_pipe.select("contigName", "start", "end", "referenceAllele", "alternateAlleles", "INFO_CSQ"))

# COMMAND ----------

single_node_vcf = spark.read.format("vcf").load(annotated_vcf)

# COMMAND ----------

display(single_node_vcf)

# COMMAND ----------

single_node_only_df = single_node_vcf.subtract(annotated_vcf_pipe)
single_node_only_count = single_node_only_df.count()

# COMMAND ----------

try:
  assert single_node_only_count == 0
except:
  raise ValueError("single node and glow VEP output do not match")

# COMMAND ----------

glow_only_df = annotated_vcf_pipe.subtract(single_node_vcf)
glow_only_count = glow_only_df.count()

# COMMAND ----------

try:
  assert glow_only_count == 0
except:
  raise ValueError("single node and glow VEP output do not match")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Let's corrupt the input!
# MAGIC 
# MAGIC To test the quarantine functionality, replace `21` contigName with `21xyz`

# COMMAND ----------

corrupted_df = df.withColumn("contigName", fx.regexp_replace('contigName', '21', "21xyz"))
display(corrupted_df)

# COMMAND ----------

display(corrupted_df.groupBy("contigName").count())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### create quarantine table in variant database

# COMMAND ----------

spark.sql("create database if not exists {}".format(variant_db_name))

# COMMAND ----------

deltaTable = (DeltaTable.createOrReplace(). \
  tableName(variant_db_quarantine_table). \
  location(output_pipe_quarantine). \
  addColumns(corrupted_df.schema). \
  execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run pipe transformer with quarantine

# COMMAND ----------

output_df = glow.transform("pipe", 
                           corrupted_df, 
                           cmd=cmd, 
                           input_formatter='vcf', 
                           in_vcf_header='infer', 
                           output_formatter='vcf',
                           quarantine_table=variant_db_quarantine_table,
                           quarantine_flavor='delta'
                          )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### the output dataframe is missing variants

# COMMAND ----------

output_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### investigate the quarantine dataframe

# COMMAND ----------

quarantine_df = spark.table(variant_db_quarantine_table)
quarantine_df = quarantine_df.join(output_df, ['contigName', 'start', 'end', 'referenceAllele', 'alternateAlleles'], "left_anti")
display(quarantine_df)

# COMMAND ----------

display(quarantine_df.groupBy("contigName").count())