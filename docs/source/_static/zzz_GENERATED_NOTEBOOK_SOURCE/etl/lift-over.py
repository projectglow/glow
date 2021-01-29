# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC To perform coordinate or variant liftover, you must download a chain file to each node. 
# MAGIC 
# MAGIC On a Databricks cluster, an example of a [cluster-scoped init script](https://docs.azuredatabricks.net/clusters/init-scripts.html#cluster-scoped-init-scripts) you can use to download the required file is as follows:
# MAGIC 
# MAGIC ```
# MAGIC #!/usr/bin/env bash
# MAGIC set -ex
# MAGIC set -o pipefail
# MAGIC mkdir /opt/liftover
# MAGIC curl https://raw.githubusercontent.com/broadinstitute/gatk/master/scripts/funcotator/data_sources/gnomAD/b37ToHg38.over.chain --output /opt/liftover/b37ToHg38.over.chain
# MAGIC ```
# MAGIC In this demo, we perform coordinate and variant liftover from b37 to hg38.
# MAGIC 
# MAGIC To perform variant liftover, you must download a reference file to each node of the cluster. Here, we assume the reference genome is downloaded to 
# MAGIC ```/mnt/dbnucleus/dbgenomics/grch38/data/GRCh38_full_analysis_set_plus_decoy_hla.fa```

# COMMAND ----------

# DBTITLE 1,Import glow and define path variables
import glow
glow.register(spark)
chain_file = '/opt/liftover/b37ToHg38.over.chain'
reference_file = '/mnt/dbnucleus/dbgenomics/grch38/data/GRCh38_full_analysis_set_plus_decoy_hla.fa'
vcf_file = 'dbfs:/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz'

# COMMAND ----------

# DBTITLE 1,First, read in a VCF from a flat file or Delta Lake table.
input_df = spark.read.format("vcf") \
  .load(vcf_file) \
  .cache()

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

from pyspark.sql.functions import *

# COMMAND ----------

liftover_expr = "lift_over_coordinates(contigName, start, end, chain_file, .99)"
input_with_lifted_df = input_df.select('contigName', 'start', 'end').withColumn('lifted', expr(liftover_expr))

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

output_df = glow.transform('lift_over_variants', input_df, chain_file=chain_file, reference_file=reference_file)

# COMMAND ----------

# DBTITLE 1,View the rows for which liftover succeeded
lifted_df = output_df.filter('liftOverStatus.success = true').drop('liftOverStatus')
display(lifted_df.select('contigName', 'start', 'end', 'referenceAllele', 'alternateAlleles', 'INFO_AC', 'INFO_SwappedAlleles', 'INFO_ReverseComplementedAlleles'))