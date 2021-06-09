# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC ### Genomics Technical Guide
# MAGIC 
# MAGIC ##### Two-way integration with github

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Syncing Glow notebooks from github to your workspace using [repos](https://docs.databricks.com/repos.html) (preview)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/add_glow_repo.png" alt="logo" width="500"/> 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### navigate to notebooks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/glow_repo_notebook_locations.png" alt="logo" width="500"/> 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### change code and then push to a branch on your fork of glow

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/glow_commit_code_repo.png" alt="logo" width="700"/> 

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Syncing Glow notebooks from github to your workspace (legacy)
# MAGIC 
# MAGIC 1. Clone Glow Github repository
# MAGIC 2. Install Databricks CLI
# MAGIC 3. Run the following bash script

# COMMAND ----------

# MAGIC %sh
# MAGIC #!/bin/bash
# MAGIC for file in /Users/williambrandler/Documents/databricks/git/glow/docs/source/_static/notebooks/*/*.html; do
# MAGIC name=${file##*/}
# MAGIC databricks workspace import $file /Users/william.brandler@databricks.com/glow/$name --overwrite --format HTML --language PYTHON --profile field-eng
# MAGIC echo $name
# MAGIC done
