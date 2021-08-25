# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC ### <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> Glow Technical Guide
# MAGIC 
# MAGIC ##### How to create production jobs programmatically

# COMMAND ----------

# MAGIC %md
# MAGIC To create a jobs cluster use the REST API, using the json below as a template
# MAGIC 
# MAGIC Note: please generate a personal access [token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-personal-access-token) and insert at `<insert_token_here>`
# MAGIC 
# MAGIC The following cluster configs are for genetic association studies

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Amazon Web Services (AWS)

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert_token_here>" -d @glow_job_create_cluster_aws.json` `https://<insert_workspace>.cloud.databricks.com/api/2.0/jobs/create`

# COMMAND ----------

# MAGIC %md
# MAGIC `glow_job_create_cluster_aws.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {
# MAGIC   "name": "glow_gwas",
# MAGIC   "notebook_task": {
# MAGIC   "notebook_path" : "/Users/<user@organization.com>/demo/genomics/glow/variant-data.html",
# MAGIC     "base_parameters": {
# MAGIC       "allele_freq_cutoff": 0.01
# MAGIC     }
# MAGIC   },
# MAGIC   "new_cluster": {
# MAGIC     "spark_version": "7.3.x-scala2.12",
# MAGIC     "aws_attributes": {
# MAGIC       "availability": "SPOT",
# MAGIC       "first_on_demand": 1
# MAGIC     },
# MAGIC     "node_type_id": "r5d.12xlarge",
# MAGIC     "driver_node_type_id": "r5d.12xlarge",
# MAGIC     "num_workers": 6,
# MAGIC     "spark_conf": {
# MAGIC       "spark.sql.execution.arrow.maxRecordsPerBatch": 100
# MAGIC     },
# MAGIC     "spark_env_vars": {
# MAGIC     }
# MAGIC   },
# MAGIC   "libraries": [
# MAGIC     {
# MAGIC       "pypi": {
# MAGIC         "package": "glow.py==1.0.1"
# MAGIC       }
# MAGIC     },
# MAGIC     {
# MAGIC       "maven": {
# MAGIC         "coordinates": "io.projectglow:glow-spark3_2.12:1.0.1"
# MAGIC       }
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ###### run job
# MAGIC 
# MAGIC example `job_id`: 5040

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert token here>" -d @glow_run_job.json` `https://<insert_workspace>.cloud.databricks.com/api/2.0/jobs/run-now`

# COMMAND ----------

# MAGIC %md
# MAGIC `glow_run_job.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {
# MAGIC   "job_id": <job_id>,
# MAGIC   "notebook_params": {
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Microsoft Azure
# MAGIC 
# MAGIC Example `workspace`: adb-984763964297111.11

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert_token_here>" -d @glow_job_create_cluster_azure.json` `https://<insert_workspace>.azuredatabricks.net/api/2.0/jobs/create`

# COMMAND ----------

# MAGIC %md
# MAGIC glow_job_create_cluster_azure.json

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {
# MAGIC   "name": "glow_gwas",
# MAGIC   "notebook_task": {
# MAGIC   "notebook_path" : "/Users/<user@organization.com>/glow/docs/source/_static/notebooks/etl/variant-data",
# MAGIC     "base_parameters": {
# MAGIC       "allele_freq_cutoff": 0.01
# MAGIC     }
# MAGIC   },
# MAGIC   "new_cluster": {
# MAGIC     "spark_version": "7.3.x-scala2.12",
# MAGIC     "azure_attributes": {
# MAGIC         "first_on_demand": 1,
# MAGIC         "availability": "SPOT_WITH_FALLBACK_AZURE",
# MAGIC         "spot_bid_max_price": -1
# MAGIC     },
# MAGIC     "node_type_id": "Standard_E8s_v3",
# MAGIC     "num_workers": 6,
# MAGIC     "spark_conf": {
# MAGIC       "spark.sql.execution.arrow.maxRecordsPerBatch": 100
# MAGIC     },
# MAGIC     "spark_env_vars": {
# MAGIC     }
# MAGIC   },
# MAGIC   "libraries": [
# MAGIC     {
# MAGIC       "pypi": {
# MAGIC         "package": "glow.py==1.0.1"
# MAGIC       }
# MAGIC     },
# MAGIC     {
# MAGIC       "maven": {
# MAGIC         "coordinates": "io.projectglow:glow-spark3_2.12:1.0.1"
# MAGIC       }
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ###### run job
# MAGIC example `job_id`: 5040
# MAGIC 
# MAGIC example `workspace`: adb-984752964297111.11

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert token here>" -d @glow_run_job.json` `https://<insert_workspace>.azuredatabricks.net.databricks.com/api/2.0/jobs/run-now`

# COMMAND ----------

# MAGIC %md
# MAGIC `glow_run_job.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {
# MAGIC   "job_id": <job_id>,
# MAGIC   "notebook_params": {
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ##### _Coming soon_: Google Cloud Platform
