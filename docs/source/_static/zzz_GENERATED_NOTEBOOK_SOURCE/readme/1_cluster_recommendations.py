# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC ### Genomics Technical Guide

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Cluster recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC 1. For ingest of variant data, use compute optimized instances
# MAGIC 2. For querying, use delta cache accelerated instances
# MAGIC 3. For genetic association studies, use memory optimized instances (more performant per core hour).
# MAGIC   - AWS, r5d series
# MAGIC   - Azure, E8s series
