# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC ### Genomics Technical Guide
# MAGIC 
# MAGIC ##### install glow

# COMMAND ----------

# MAGIC %md
# MAGIC ###### [Glow](https://www.projectglow.io/) - install via PyPi and Maven
# MAGIC 
# MAGIC <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/environment/install_glow.png" alt="logo" width="500"/> 

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert_token>" -d @install_glow.json` `https://<insert_workspace>.cloud.databricks.com/api/2.0/libraries/install`

# COMMAND ----------

# MAGIC %md
# MAGIC ##### install_glow.json 

# COMMAND ----------

{
  "cluster_id": "<insert_cluster_id>",
  "libraries": [
    {
      "pypi": {
        "package": "bioinfokit==0.8.5"
      }
    },
    {
      "pypi": {
        "package": "glow.py==1.0.1"
      }
    },
    {
      "maven": {
        "coordinates": "io.projectglow:glow-spark3_2.12:1.0.1"
      }
    }
  ]
}
