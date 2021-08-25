# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC ### <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> Glow Technical Guide

# COMMAND ----------

# MAGIC %md
# MAGIC ##### <img src="https://hail.is/static/hail-logo-cropped-sm-opt.png" alt="logo" width="50"/>

# COMMAND ----------

# MAGIC %md
# MAGIC ###### [Hail](https://hail.is/docs/0.2/install/other-cluster.html) - install via an init script

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. put init script on the databricks file system (dbfs)

# COMMAND ----------

dbutils.fs.put(
'dbfs:/tmp/databricks/scripts/install-hail.sh',
'''
#!/bin/bash
set -ex

# Pick up user-provided environment variables, specifically HAIL_VERSION
source /databricks/spark/conf/spark-env.sh

/databricks/python/bin/pip install -U hail==$HAIL_VERSION
hail_jar_path=$(find /databricks/python3 -name 'hail-all-spark.jar')
cp $hail_jar_path /databricks/jars

# Note: This configuration takes precedence since configurations are
# applied in reverse-lexicographic order.
cat <<HERE >/databricks/driver/conf/00-hail.conf
[driver] {
  "spark.kryo.registrator" = "is.hail.kryo.HailKryoRegistrator"
  "spark.hadoop.fs.s3a.connection.maximum" = 5000
  "spark.serializer" = "org.apache.spark.serializer.KryoSerializer"
}
HERE

echo $?
''',
  overwrite = True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. setup a cluster that points to the init script

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/environment/install_hail_init.png" alt="logo" width="600"/> 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 3. set spark environment variable

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/environment/install_hail_env.png" alt="logo" width="600"/> 
