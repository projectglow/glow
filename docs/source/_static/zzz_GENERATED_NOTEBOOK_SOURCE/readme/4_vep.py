# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC ### <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> Glow Technical Guide
# MAGIC 
# MAGIC ##### Install the variant effect predictor ([VEP](https://github.com/Ensembl/ensembl-vep)) with [Databricks container services](https://docs.databricks.com/clusters/custom-containers.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### From your laptop build and push Dockerfile

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC cd vep
# MAGIC docker build -f Dockerfile . 
# MAGIC docker build -t projectglow/vep . 
# MAGIC docker push projectglow/vep:latest
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Find the latest version of this Docker container at,
# MAGIC 
# MAGIC https://hub.docker.com/r/projectglow/vep

# COMMAND ----------

# MAGIC %md
# MAGIC Dockerfile

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC FROM databricksruntime/standard:latest 
# MAGIC   
# MAGIC ARG DEBIAN_FRONTEND=noninteractive
# MAGIC 
# MAGIC ENV TZ=America/New_York
# MAGIC 
# MAGIC RUN apt-get update && apt-get install -y \ 
# MAGIC     build-essential \
# MAGIC     git \
# MAGIC     apt-transport-https \
# MAGIC     ca-certificates \
# MAGIC     cpanminus \ 
# MAGIC     libpng-dev \ 
# MAGIC     zlib1g-dev \ 
# MAGIC     libbz2-dev \ 
# MAGIC     liblzma-dev \ 
# MAGIC     perl \ 
# MAGIC     perl-base \ 
# MAGIC     unzip \
# MAGIC     curl \
# MAGIC     gnupg2 \ 
# MAGIC     software-properties-common \ 
# MAGIC     jq \
# MAGIC     libjemalloc1 \ 
# MAGIC     libjemalloc-dev \ 
# MAGIC     libdbi-perl \ 
# MAGIC     libdbd-mysql-perl \ 
# MAGIC     libdbd-sqlite3-perl
# MAGIC # ===== Set up VEP environment =====================================================================
# MAGIC ENV OPT_SRC /opt/vep/src
# MAGIC 
# MAGIC ENV PERL5LIB $PERL5LIB:$OPT_SRC/ensembl-vep:$OPT_SRC/ensembl-vep/modules
# MAGIC RUN cpanm DBI && \
# MAGIC     cpanm Set::IntervalTree && \
# MAGIC     cpanm JSON && \
# MAGIC     cpanm Text::CSV && \
# MAGIC     cpanm Module::Build && \
# MAGIC     cpanm PerlIO::gzip && \
# MAGIC     cpanm IO::Uncompress::Gunzip
# MAGIC                         
# MAGIC RUN mkdir -p $OPT_SRC
# MAGIC 
# MAGIC WORKDIR $OPT_SRC
# MAGIC 
# MAGIC RUN git clone https://github.com/Ensembl/ensembl-vep.git WORKDIR ensembl-vep
# MAGIC   
# MAGIC # The commit is the most recent one on release branch 100 as of July 29, 2020
# MAGIC RUN git checkout 10932fab1e9c113e8e5d317e1f668413390344ac && \
# MAGIC     perl INSTALL.pl --NO_UPDATE -AUTO a && \
# MAGIC     perl INSTALL.pl -n -a p --PLUGINS AncestralAllele && \ 
# MAGIC     chmod +x vep
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Go to your DockerHub page and you will see the repository ([example](https://hub.docker.com/r/projectglow/vep))
# MAGIC 
# MAGIC Now, set up a cluster on the demo shard with the Docker Image URL
# MAGIC 
# MAGIC <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/vep_docker_container_cluster.png" alt="logo" width="700"/>

# COMMAND ----------

# MAGIC %md
# MAGIC Or Create Cluster using API:

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Amazon Web Services (AWS)

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert_token>" -d @create_cluster_dcs_vep_aws.json`
# MAGIC `https://<insert_workspace>.cloud.databricks.com/api/2.0/clusters/create`

# COMMAND ----------

# MAGIC %md
# MAGIC create_cluster_dcs_vep_aws.json

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {
# MAGIC   "cluster_name": "vep-glow",
# MAGIC   "spark_version": "7.3.x-scala2.12",
# MAGIC   "num_workers": 1,
# MAGIC   "node_type_id": "i3.xlarge",
# MAGIC   "driver_node_type_id": "i3.xlarge",
# MAGIC   "autotermination_minutes": 60,
# MAGIC   "enable_elastic_disk": true,
# MAGIC   "docker_image": {
# MAGIC     "url": "projectglow/vep:latest"
# MAGIC   },
# MAGIC   "aws_attributes": {
# MAGIC     "first_on_demand": 1,
# MAGIC     "availability": "SPOT_WITH_FALLBACK",
# MAGIC     "spot_bid_price_percent": 100,
# MAGIC     "zone_id": "us-west-2a"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Cluster creation will return the cluster_id, now layer on libraries to this cluster,
# MAGIC 
# MAGIC Example cluster_id: `0510-162907-upped13`

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert_token>" -d @install_glow.json` `https://<insert_workspace>.cloud.databricks.com/api/2.0/libraries/install`

# COMMAND ----------

# MAGIC %md
# MAGIC install_glow.json

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {
# MAGIC   "cluster_id": "<cluster_id>",
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
# MAGIC `curl -X POST -H "Authorization: Bearer <insert_token>" -d @create_cluster_dcs_vep_azure.json`
# MAGIC `https://<insert_workspace>.azuredatabricks.net/api/2.0/clusters/create`

# COMMAND ----------

# MAGIC %md
# MAGIC create_cluster_dcs_vep_azure.json

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC {
# MAGIC   "cluster_name": "vep-glow",
# MAGIC   "spark_version": "7.3.x-scala2.12",
# MAGIC   "num_workers": 1,
# MAGIC   "node_type_id": "Standard_F8s_v2",
# MAGIC   "autotermination_minutes": 60,
# MAGIC   "enable_elastic_disk": true,
# MAGIC   "docker_image": {
# MAGIC     "url": "projectglow/vep:latest"
# MAGIC   },
# MAGIC   "azure_attributes": {
# MAGIC         "first_on_demand": 1,
# MAGIC         "availability": "SPOT_WITH_FALLBACK_AZURE",
# MAGIC         "spot_bid_max_price": -1
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ##### _Coming Soon_ Google Cloud Platform (GCP)
