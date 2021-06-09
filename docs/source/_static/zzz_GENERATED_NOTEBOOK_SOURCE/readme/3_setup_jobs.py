# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC ### Genomics Technical Guide
# MAGIC 
# MAGIC ##### How to create jobs programmatically

# COMMAND ----------

# MAGIC %md
# MAGIC To create a jobs cluster use the REST API, using the json below as a template. Note: please generate a personal access [token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-personal-access-token) first and insert it.
# MAGIC 
# MAGIC The following cluster configs are for genetic association studies

# COMMAND ----------

# MAGIC %md
# MAGIC ##### AWS

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert token here>" -d @glow_job_create_cluster_aws.json` `https://<insert_workspace_id>.cloud.databricks.com/api/2.0/jobs/create`

# COMMAND ----------

glow_job_create_cluster_aws.json

# COMMAND ----------

{
  "name": "glow",
  "notebook_task": {
  "notebook_path" : "/Users/william.brandler@databricks.com/demo/genomics/glow/variant-data.html",
    "base_parameters": {
      "allele_freq_cutoff": 0.01
    }
  },
  "new_cluster": {
    "spark_version": "7.3.x-scala2.12",
    "aws_attributes": {
      "availability": "SPOT",
      "first_on_demand": 1
    },
    "node_type_id": "r5d.12xlarge",
    "driver_node_type_id": "r5d.12xlarge",
    "num_workers": 6,
    "spark_env_vars": {
    }
  },
  "libraries": [
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


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Azure

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert token here>" -d @glow_job_create_cluster_azure.json` `https://<insert_workspace_id>.cloud.databricks.com/api/2.0/jobs/create`

# COMMAND ----------

# MAGIC %md
# MAGIC glow_job_create_cluster_azure.json

# COMMAND ----------

{
    "num_workers": 24,
    "cluster_name": "",
    "spark_version": "7.3.x-scala2.12",
    "spark_conf": {
        "spark.databricks.delta.preview.enabled": "true"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "SPOT_WITH_FALLBACK_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_E8s_v3",
    "ssh_public_keys": [],
    "custom_tags": {},
    "spark_env_vars": {},
    "enable_elastic_disk": true,
    "cluster_source": "JOB",
    "init_scripts": []
}

# COMMAND ----------

# MAGIC %md
# MAGIC ###### run job

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert token here>" -d @glow_run_job.json` `https://<insert_workspace_here>.cloud.databricks.com/api/2.0/jobs/run-now`

# COMMAND ----------

glow_run_job.json

# COMMAND ----------

{
  "job_id": 5040,
  "notebook_params": {
  }
}
