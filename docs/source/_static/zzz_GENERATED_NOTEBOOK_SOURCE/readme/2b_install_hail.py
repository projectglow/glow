# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC ### Genomics Technical Guide
# MAGIC 
# MAGIC ##### Install Hail

# COMMAND ----------

# MAGIC %md
# MAGIC ###### [Hail](https://hail.is/docs/0.2/install/other-cluster.html) - install via an init script
# MAGIC 
# MAGIC Please see this page on the [docs](https://docs.databricks.com/applications/genomics/genomics-libraries/hail.html). 
# MAGIC 
# MAGIC Follow the instructions in the section initialization scripts, then see the screenshots and script below to configure the cluster
# MAGIC 
# MAGIC <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/environment/install_hail.png" alt="logo" width="500"/> 
# MAGIC 
# MAGIC <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/environment/install_hail_environment_variable.png" alt="logo" width="400"/> 

# COMMAND ----------

# MAGIC %md
# MAGIC install_hail.sh

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC #!/bin/bash
# MAGIC set -ex
# MAGIC 
# MAGIC # Pick up user-provided environment variables, specifically HAIL_VERSION
# MAGIC source /databricks/spark/conf/spark-env.sh
# MAGIC 
# MAGIC /databricks/python/bin/pip install -U hail==$HAIL_VERSION
# MAGIC hail_jar_path=$(find /databricks/python3 -name 'hail-all-spark.jar')
# MAGIC cp $hail_jar_path /databricks/jars
# MAGIC 
# MAGIC # Note: This configuration takes precedence since configurations are
# MAGIC # applied in reverse-lexicographic order.
# MAGIC cat <<HERE >/databricks/driver/conf/00-hail.conf
# MAGIC [driver] {
# MAGIC   "spark.kryo.registrator" = "is.hail.kryo.HailKryoRegistrator"
# MAGIC   "spark.hadoop.fs.s3a.connection.maximum" = 5000
# MAGIC   "spark.serializer" = "org.apache.spark.serializer.KryoSerializer"
# MAGIC }
# MAGIC HERE
# MAGIC 
# MAGIC echo $?
# MAGIC ```
