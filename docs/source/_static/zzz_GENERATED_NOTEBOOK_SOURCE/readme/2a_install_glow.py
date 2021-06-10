# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC ### Genomics Technical Guide
# MAGIC 
# MAGIC ##### install glow on existing cluster
# MAGIC 
# MAGIC Note: requires a personal access token ([docs](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-personal-access-token))
# MAGIC 
# MAGIC Note: please insert `<cluster_id>` where prompted
# MAGIC 
# MAGIC example cluster_id: `0510-162907-upped13`

# COMMAND ----------

# MAGIC %md
# MAGIC ###### [Glow](https://www.projectglow.io/) - install via PyPi and Maven
# MAGIC 
# MAGIC <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/environment/install_glow.png" alt="logo" width="500"/> 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Amazon Web Services (AWS)

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert_token>" -d @install_glow.json` `https://<insert_workspace>.cloud.databricks.com/api/2.0/libraries/install`

# COMMAND ----------

# MAGIC %md
# MAGIC ##### install_glow_aws.json 
# MAGIC 
# MAGIC <insert_cluster_id>

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {
# MAGIC   "cluster_id": "<insert_cluster_id>",
# MAGIC   "libraries": [
# MAGIC     {
# MAGIC       "pypi": {
# MAGIC         "package": "bioinfokit==0.8.5"
# MAGIC       }
# MAGIC     },
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
# MAGIC ##### Microsoft Azure
# MAGIC 
# MAGIC Example `workspace`: adb-984763964297111.12

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert_token>" -d @install_glow_azure.json` `https://<insert_workspace>.azuredatabricks.net/api/2.0/libraries/install`

# COMMAND ----------

# MAGIC %md
# MAGIC ##### install_glow_azure.json 
# MAGIC 
# MAGIC <insert_cluster_id>

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {
# MAGIC   "cluster_id": "<insert_cluster_id>",
# MAGIC   "libraries": [
# MAGIC     {
# MAGIC       "pypi": {
# MAGIC         "package": "bioinfokit==0.8.5"
# MAGIC       }
# MAGIC     },
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
