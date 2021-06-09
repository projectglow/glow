# Databricks notebook source
# MAGIC %md
# MAGIC ##<img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> 
# MAGIC ### Genomics Technical Guide
# MAGIC 
# MAGIC ##### [Databricks container services](https://docs.databricks.com/clusters/custom-containers.html)

# COMMAND ----------

# MAGIC %sh
# MAGIC cd vep
# MAGIC docker build -f Dockerfile . 
# MAGIC docker build -t projectglow/vep . 
# MAGIC docker push projectglow/vep:latest

# COMMAND ----------

# MAGIC %md
# MAGIC Dockerfile

# COMMAND ----------

FROM databricksruntime/standard:latest 
  
ARG DEBIAN_FRONTEND=noninteractive

ENV TZ=America/New_York

RUN apt-get update && apt-get install -y \ 
    build-essential \
    git \
    apt-transport-https \
    ca-certificates \
    cpanminus \ 
    libpng-dev \ 
    zlib1g-dev \ 
    libbz2-dev \ 
    liblzma-dev \ 
    perl \ 
    perl-base \ 
    unzip \
    curl \
    gnupg2 \ 
    software-properties-common \ 
    jq \
    libjemalloc1 \ 
    libjemalloc-dev \ 
    libdbi-perl \ 
    libdbd-mysql-perl \ 
    libdbd-sqlite3-perl
# ===== Set up VEP environment =====================================================================
ENV OPT_SRC /opt/vep/src

ENV PERL5LIB $PERL5LIB:$OPT_SRC/ensembl-vep:$OPT_SRC/ensembl-vep/modules
RUN cpanm DBI && \
    cpanm Set::IntervalTree && \
    cpanm JSON && \
    cpanm Text::CSV && \
    cpanm Module::Build && \
    cpanm PerlIO::gzip && \
    cpanm IO::Uncompress::Gunzip
                        
RUN mkdir -p $OPT_SRC

WORKDIR $OPT_SRC

RUN git clone https://github.com/Ensembl/ensembl-vep.git WORKDIR ensembl-vep
  
# The commit is the most recent one on release branch 100 as of July 29, 2020
RUN git checkout 10932fab1e9c113e8e5d317e1f668413390344ac && \
    perl INSTALL.pl --NO_UPDATE -AUTO a && \
    perl INSTALL.pl -n -a p --PLUGINS AncestralAllele && \ 
    chmod +x vep

# COMMAND ----------

# MAGIC %md
# MAGIC Go to your DockerHub page and you should see the repository there ([example](https://cloud.docker.com/repository/docker/wbrandler/vep))
# MAGIC 
# MAGIC Now, set up a cluster on the demo shard with the Docker Image URL
# MAGIC 
# MAGIC <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/environment/install_docker.png" alt="logo" width="400"/>

# COMMAND ----------

# MAGIC %md
# MAGIC Or Create Cluster using API

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert_token>" -d @create_cluster_dcs_aws_vep.json`
# MAGIC `https://<insert_workspace>.cloud.databricks.com/api/2.0/clusters/create`

# COMMAND ----------

# MAGIC %md
# MAGIC create_cluster_dcs_aws_vep.json

# COMMAND ----------

{
  "cluster_name": "vep-glow",
  "spark_version": "7.3.x-scala2.12",
  "num_workers": 1,
  "node_type_id": "i3.xlarge",
  "driver_node_type_id": "i3.xlarge",
  "autotermination_minutes": 60,
  "enable_elastic_disk": true,
  "docker_image": {
    "url": "projectglow/vep:latest"
  },
  "aws_attributes": {
    "first_on_demand": 1,
    "availability": "SPOT_WITH_FALLBACK",
    "spot_bid_price_percent": 100,
    "zone_id": "us-west-2a"
  },
  "spark_conf": {
        "spark.sql.execution.arrow.maxRecordsPerBatch": 100
  }
}

# COMMAND ----------

# MAGIC %md
# MAGIC Cluster creation will return the cluster_id, now layer on libraries to this cluster,

# COMMAND ----------

# MAGIC %md
# MAGIC `curl -X POST -H "Authorization: Bearer <insert_token>" -d @install_glow.json` `https://<insert_workspace>.cloud.databricks.com/api/2.0/libraries/install`

# COMMAND ----------

# MAGIC %md
# MAGIC install_glow.json

# COMMAND ----------

{
  "cluster_id": "0510-162907-upped13",
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
