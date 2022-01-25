# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Download 1000G variant data

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../../0_setup_constants_glow

# COMMAND ----------

# MAGIC %md
# MAGIC ##### download 1000G data for chrom 21 and 22

# COMMAND ----------

# MAGIC %sh
# MAGIC declare -a chroms=("1" "2" "3" "4" "5" "6" "7" "8" "9" "10" "11" "12" "13" "14" "15" "16" "17" "18" "19" "21" "22")
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
                              .where(fx.col("INFO_AF")[0] >= allele_freq_cutoff)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### checkpoint to delta

# COMMAND ----------

vcf.write.mode("overwrite").format("delta").save(output_vcf_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read back in and count data

# COMMAND ----------

display(spark.read.format("delta").load(output_vcf_delta). \
                                   groupBy("contigName").count(). \
                                   sort("contigName"))