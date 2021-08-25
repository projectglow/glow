# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC ### <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> Glow Technical Guide
# MAGIC 
# MAGIC ##### [Initialization](https://docs.databricks.com/clusters/init-scripts.html#cluster-node-initialization-scripts) scripts for single node tools

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Note: Cluster node init scripts, which run during startup for each cluster node before the Spark driver or worker JVM starts
# MAGIC 
# MAGIC Note: PyPi, CRAN or Docker are preferred over init scripts
# MAGIC 
# MAGIC Upload the init script to dbfs with the Databricks file system command line interface ([CLI](https://docs.databricks.com/dev-tools/databricks-cli.html#dbfs-cli))
# MAGIC 
# MAGIC `databricks fs cp vep_install.sh dbfs:/mnt/home/init/vep_install.sh --profile <insert_workspace_id>`

# COMMAND ----------

# MAGIC %md
# MAGIC or run the commands below on a cluster

# COMMAND ----------

user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
file_path = "dbfs:/mnt/home/" + user + "/init/"
dbutils.fs.mkdirs(file_path)

# COMMAND ----------

dbutils.fs.put(file_path + "vep_install.sh",""" #!/bin/bash
apt-get update
apt-get install -y build essential
apt-get install -y apt-transport-https ca-certificates curl gnupg2 software-properties-common tabix unzip jq libjemalloc1 libjemalloc-dev cpanminus libdbi-perl libdbd-mysql-perl libdbd-sqlite3-perl
mkdir /opt/vep
mkdir /opt/vep/src
cd /opt/vep/src
cpanm DBI
cpanm JSON
cpanm Module::Build
cpanm Archive::Zip
cpanm DBD::mysql
cd /opt/vep/src/
git clone https://github.com/Ensembl/ensembl-vep.git
cd /opt/vep/src/ensembl-vep
git checkout release/100
export PERL5LIB=${PERL5LIB}:/opt/vep/src/ensembl-vep:/opt/vep/src/ensembl-vep/modu les
perl INSTALL.pl -a a -n
perl INSTALL.pl -n -a p --PLUGINS AncestralAllele
chmod +x vep
""", True)

# COMMAND ----------

# MAGIC %md
# MAGIC add to a cluster

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://docs.databricks.com/_images/init-scripts-aws.png" alt="logo" width="500"/>
