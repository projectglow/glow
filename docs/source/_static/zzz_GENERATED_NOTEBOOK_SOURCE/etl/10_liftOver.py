# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC To perform coordinate or variant liftover, you must download a chain file to each node.
# MAGIC 
# MAGIC For a Databricks cluster, please use the [glow docker container](https://github.com/projectglow/glow/blob/708440a8ed9fc38b74f93058f497c9d95313e9f9/docker/databricks/dbr/dbr9.1/genomics-with-glow/Dockerfile#L45-L46) with the files already included.
# MAGIC 
# MAGIC Use [Databricks Container Services](https://docs.databricks.com/clusters/custom-containers.html#launch-your-cluster-using-the-ui) to pull the latest version of the container from [projectglow's dockerhub](https://hub.docker.com/u/projectglow)
# MAGIC 
# MAGIC However, this does not include the genome builds, an example of which is avaialble on cloud storage under `/dbfs/databricks-datasets/genomics`
# MAGIC 
# MAGIC In this demo, we perform coordinate and variant liftover from b37 to hg38.
# MAGIC 
# MAGIC To perform variant liftover, you must download a reference file to each node of the cluster. Here, we use the FUSE mount to access the reference genome at
# MAGIC ```/dbfs/databricks-datasets/genomics/grch38/data/GRCh38_full_analysis_set_plus_decoy_hla.fa```

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

method = 'etl'
test = 'liftOver'
library = 'glow'
datetime = datetime.now(pytz.timezone('US/Pacific'))

# COMMAND ----------

# DBTITLE 1,Import glow and define path variables
chain_file = '/opt/liftover/b37ToHg38.over.chain'
reference_file = '/dbfs/databricks-datasets/genomics/grch38/data/GRCh38_full_analysis_set_plus_decoy_hla.fa'

# COMMAND ----------

# DBTITLE 1,First, read in a VCF from a flat file or Delta Lake table.
input_df = (spark.read.format("vcf")
  .load(output_vcf)
  .drop("genotypes")
  .cache())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now apply the `lift_over_coordinates` UDF, with the parameters as follows:
# MAGIC - chromosome (`string`)
# MAGIC - start (`long`)
# MAGIC - end (`long`)
# MAGIC - CONSTANT: chain file (`string`)
# MAGIC - OPTIONAL: minimum fraction of bases that must remap (`double`), defaults to `.95`
# MAGIC 
# MAGIC This creates a column with the new coordinates.

# COMMAND ----------

liftover_expr = f"lift_over_coordinates(contigName, start, end, '{chain_file}', .99)"
input_with_lifted_df = input_df.select('contigName', 'start', 'end').withColumn('lifted', fx.expr(liftover_expr))

# COMMAND ----------

# DBTITLE 1,Filter rows for which liftover succeeded and see which rows changed.
changed_with_lifted_df = input_with_lifted_df.filter("lifted is not null").filter("start != lifted.start")
display(changed_with_lifted_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now apply the `lift_over_variants` transformer, with the following options.
# MAGIC - `chain_file`: `string`
# MAGIC - `reference_file`: `string`
# MAGIC - `min_match_ratio`: `double` (optional, defaults to `.95`)

# COMMAND ----------

start_time = time.time()

# COMMAND ----------

output_df = glow.transform('lift_over_variants', input_df, chain_file=chain_file, reference_file=reference_file)

# COMMAND ----------

# DBTITLE 1,View the rows for which liftover succeeded
lifted_df = output_df.filter('liftOverStatus.success = true').drop('liftOverStatus')
lifted_df.write.format("delta").mode("overwrite").save(output_liftOver_delta)

# COMMAND ----------

end_time = time.time()
log_metadata(datetime, n_samples, n_variants, 0, 0, method, test, library, spark_version, node_type_id, n_workers, start_time, end_time, run_metadata_delta_path)

# COMMAND ----------

lifted_df = spark.read.format("delta").load(output_liftOver_delta)
display(lifted_df.select('contigName', 'start', 'end', 'referenceAllele', 'alternateAlleles', 'INFO_AC', 'INFO_SwappedAlleles', 'INFO_ReverseComplementedAlleles'))

# COMMAND ----------

lifted_df.count()