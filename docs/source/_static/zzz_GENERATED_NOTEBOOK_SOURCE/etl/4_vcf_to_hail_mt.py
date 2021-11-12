# Databricks notebook source
# MAGIC %md
# MAGIC ### Convert VCF file to hail matrix table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### run notebook(s) to set everything up

# COMMAND ----------

# MAGIC %run ../1_setup_constants_hail

# COMMAND ----------

# MAGIC %md
# MAGIC ##### prepare hail matrix table

# COMMAND ----------

mt = hl.import_vcf(input_vcf, reference_genome='GRCh37')
mt.show()

# COMMAND ----------

mt.count()

# COMMAND ----------

mt.repartition(n_partitions_hail).write(hail_matrix_table_outpath, overwrite=True)