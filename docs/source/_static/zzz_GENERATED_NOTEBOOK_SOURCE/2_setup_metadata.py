# Databricks notebook source
# MAGIC %md
# MAGIC ##### run metadata
# MAGIC 
# MAGIC Warning: this will only work in Databricks
# MAGIC 
# MAGIC It requires the databricks-cli to be installed via pip 
# MAGIC 
# MAGIC (this is included in the `projectglow/databricks-glow` docker container)

# COMMAND ----------

# MAGIC %run ./0_setup_constants_glow

# COMMAND ----------

import pyspark.sql.functions as fx
from pyspark.sql.types import *
from databricks_cli.sdk.service import JobsService
from databricks_cli.sdk.service import ClusterService
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import get_config

from pathlib import Path

# COMMAND ----------

dbfs_home_path = Path("dbfs:/home/{}/".format(user))
run_metadata_delta_path = str(dbfs_home_path / "genomics/data/delta/pipeline_runs_info_hail_glow.delta")

# COMMAND ----------

cluster_id=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('clusterId')

# COMMAND ----------

cs = ClusterService(_get_api_client(get_config()))
_list = cs.list_clusters()['clusters']
conv = lambda x: {c:v for c,v in x.items() if type(v) in (str, int)}
cluster_info = spark.createDataFrame([conv(x) for x in _list])
cluster_info = cluster_info.where(fx.col("cluster_id") == cluster_id)
worker_info = cluster_info.select("node_type_id", "num_workers", "spark_version", "creator_user_name").collect()
node_type_id = worker_info[0].node_type_id
n_workers = worker_info[0].num_workers
spark_version = worker_info[0].spark_version
creator_user_name = worker_info[0].creator_user_name

# COMMAND ----------

display(cluster_info)

# COMMAND ----------

print("spark_version = " + str(spark_version))
print("node_type_id = " + str(node_type_id))
print("n_workers = " + str(n_workers))
print("creator_user_name = " + str(creator_user_name))

# COMMAND ----------

#define schema for logging runs in delta lake
schema = StructType([StructField("datetime", DateType(), True),
                     StructField("n_samples", LongType(), True),
                     StructField("n_variants", LongType(), True),
                     StructField("n_covariates", LongType(), True),
                     StructField("n_phenotypes", LongType(), True),
                     StructField("method", StringType(), True),
                     StructField("test", StringType(), True),
                     StructField("library", StringType(), True),
                     StructField("spark_version", StringType(), True),
                     StructField("node_type_id", StringType(), True),
                     StructField("n_workers", LongType(), True),
                     StructField("n_cores", LongType(), True),
                     StructField("runtime", DoubleType(), True)
                    ])

# COMMAND ----------

#TODO add GCP
node_to_core_mapping = {"r5d.2xlarge": 8,
                        "r5d.4xlarge": 16, 
                        "c5d.2xlarge": 8, 
                        "Standard_DS3_v2": 4,
                        "Standard_DS4_v2": 8,
                        "Standard_E8s_v3": 8,
                        "Standard_L8s_v2": 8,
                        "Standard_L64s_v2": 64
                       } 

# COMMAND ----------

def lookup_cores(node_type_id, n_workers, node_to_core_mapping=node_to_core_mapping):
  cores_per_node = node_to_core_mapping[node_type_id]
  total_cores = cores_per_node * n_workers
  return total_cores

def log_metadata(datetime, n_samples, n_variants, n_covariates, n_binary_phenotypes, method, test, library, spark_version, node_type_id, n_workers, start_time, end_time, run_metadata_delta_path):
  """
  log metadata about each step in the pipeline and append to delta lake table
  """
  runtime = float("{:.2f}".format((end_time - start_time)))
  n_cores = lookup_cores(node_type_id, n_workers, node_to_core_mapping=node_to_core_mapping)
  l = [(datetime, n_samples, n_variants, n_covariates, n_binary_phenotypes, method, test, library, spark_version, node_type_id, n_workers, n_cores, runtime)]
  run_metadata_delta_df = spark.createDataFrame(l, schema=schema)
  run_metadata_delta_df.write.mode("append").format("delta").save(run_metadata_delta_path)