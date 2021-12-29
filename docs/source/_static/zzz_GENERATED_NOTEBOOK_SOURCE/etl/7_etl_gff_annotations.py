# Databricks notebook source
# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read in gff file

# COMMAND ----------

gff_path = "/databricks-datasets/genomics/gffs/GCF_000001405.39_GRCh38.p13_genomic.gff.bgz"
original_gff_df = spark.read. \
  format("gff"). \
  load(gff_path)

# COMMAND ----------

display(original_gff_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### select columns of interest

# COMMAND ----------

etl_original_gff_df = original_gff_df.select(['start', 'end', 'type', 'seqId', 'source', 
                                              'strand', 'phase', 'ID', 'gene', 'product', 'transcript_id'])

# COMMAND ----------

display(etl_original_gff_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### annotate chromosome (contigName) to gff dataframe
# MAGIC 
# MAGIC by selecting regions and joining back to original dataframe

# COMMAND ----------

regions_df = original_gff_df.where((fx.col("type") == "region") & (fx.col("genome") == "chromosome")). \
                             select("seqId", "chromosome"). \
                             withColumnRenamed("chromosome", "contigName")
display(regions_df)

# COMMAND ----------

regions_df.count()

# COMMAND ----------

etl_original_gff_chrom_df = etl_original_gff_df.join(regions_df, "seqId", "inner")

# COMMAND ----------

display(etl_original_gff_chrom_df)

# COMMAND ----------

etl_original_gff_chrom_df.write.mode("overwrite").format("delta").save(gff_annotations)