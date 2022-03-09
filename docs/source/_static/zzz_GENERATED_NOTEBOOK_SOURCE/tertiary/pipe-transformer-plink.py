# Databricks notebook source
# MAGIC %md
# MAGIC ### Run plink using the <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> [Pipe Transformer](https://glow.readthedocs.io/en/latest/tertiary/pipe-transformer.html)
# MAGIC 
# MAGIC Plink and Glow are installed via the [Glow Docker Container](https://hub.docker.com/u/projectglow)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run notebook(s) to set everything up

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check plink is installed across the cluster

# COMMAND ----------

# MAGIC %sh
# MAGIC /opt/plink-1.07-x86_64/plink --noweb

# COMMAND ----------

# MAGIC %sh
# MAGIC /opt/plink

# COMMAND ----------

# MAGIC %md
# MAGIC #### load 1000 Genomes VCF

# COMMAND ----------

vcf_df = spark.read.format("vcf").load(output_vcf_small)

# COMMAND ----------

vcf_df.rdd.getNumPartitions()

# COMMAND ----------

display(vcf_df.drop("genotypes"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### run plink on a VCF file with the pipe transformer!
# MAGIC 
# MAGIC Here we are going to convert the VCF to plink and calculate the allele frequency for each SNP

# COMMAND ----------

scriptFile = r"""#!/bin/sh
set -e

#write partitioned VCF
export tmpdir=$(mktemp -d -t vcf.XXXXXX)
cat - > ${tmpdir}/input.vcf

#convert to plink
/opt/plink --vcf ${tmpdir}/input.vcf --keep-allele-order --make-bed --silent --out ${tmpdir}/plink

#run plink
/opt/plink-1.07-x86_64/plink --bfile ${tmpdir}/plink --freq --noweb --silent --out ${tmpdir}/plink

cat ${tmpdir}/plink.frq | sed -e 's/ \+/,/g' | sed 's/,//'
"""

cmd = json.dumps(["bash", "-c", scriptFile])

# COMMAND ----------

plink_freq_df = glow.transform('pipe', 
                               vcf_df, 
                               cmd=cmd, 
                               input_formatter='vcf',
                               in_vcf_header='infer',
                               output_formatter='csv',
                               out_header='true')

# COMMAND ----------

display(plink_freq_df)

# COMMAND ----------

plink_freq_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run plink using binary ped file as the starting input

# COMMAND ----------

# MAGIC %md
# MAGIC ##### generate a plink binary ped file from VCF for testing

# COMMAND ----------

# MAGIC %sh
# MAGIC /opt/plink --vcf $output_vcf --keep-allele-order --make-bed --out $output_vcf.genotypes --noweb

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read in the plink binary ped file

# COMMAND ----------

df_bed = spark.read.format("plink").load(output_vcf_small + ".genotypes.bed")

# COMMAND ----------

display(df_bed)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run the pipe transformer!
# MAGIC 
# MAGIC Since the plink and vcf schemas are unified, you can pipe the binary ped dataframe as `'vcf'`

# COMMAND ----------

plink_bed_freq_df = glow.transform('pipe', 
                                   df_bed.drop("position"), 
                                   cmd=cmd, 
                                   input_formatter='vcf',
                                   in_vcf_header='infer',
                                   output_formatter='csv',
                                   out_header='true')

# COMMAND ----------

display(plink_bed_freq_df)