# Databricks notebook source
import pyspark.sql.functions as fx
from pyspark.sql.types import *
import glow
spark = glow.register(spark)
import json

# COMMAND ----------

# DBTITLE 1,Use the text input and output formatters
df = spark.createDataFrame([["foo"], ["bar"], ["baz"]], ["text"])
display(glow.transform('pipe', df, cmd=['rev'], input_formatter='text', output_formatter='text'))

# COMMAND ----------

# DBTITLE 1,Read 1kg chr22
df = spark.read.format("vcf").option("flattenInfoFields", False).load("/databricks-datasets/genomics/1kg-vcfs/*.vcf.gz")
df = sqlContext.createDataFrame(sc.parallelize(df.take(1000)), df.schema).cache()

# COMMAND ----------

# DBTITLE 1,Use grep to drop INFO lines from VCF header
transformed_df = glow.transform('pipe', df, cmd=["grep", "-v", "#INFO"],
              input_formatter = 'vcf', in_vcf_header='infer', output_formatter='vcf')

# COMMAND ----------

# DBTITLE 1,Transformed DF does not have INFO fields
display(transformed_df.drop("genotypes"))

# COMMAND ----------

# DBTITLE 1,Using the Scala API
# MAGIC %scala
# MAGIC import io.projectglow.Glow
# MAGIC 
# MAGIC val df = spark.read.format("vcf").load("/databricks-datasets/genomics/1kg-vcfs/*.vcf.gz").limit(10)
# MAGIC val transformed = Glow.transform("pipe", df, Map(
# MAGIC            "cmd" -> Seq("grep", "-v", "#INFO"),
# MAGIC            "inputFormatter" -> "vcf",
# MAGIC            "outputFormatter" -> "vcf",
# MAGIC            "inVcfHeader" -> "infer")) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run bedtools using the <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> [Pipe Transformer](https://glow.readthedocs.io/en/latest/tertiary/pipe-transformer.html)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install bedtools across cluster
# MAGIC 
# MAGIC The init script downloads bedtools and installs it across the Spark cluster on Databricks
# MAGIC 
# MAGIC Upload the init script to dbfs with the Databricks file system [CLI](https://docs.databricks.com/dev-tools/databricks-cli.html#dbfs-cli), 
# MAGIC 
# MAGIC or run the command below on a different cluster
# MAGIC 
# MAGIC Note: Cluster node [init scripts](https://docs.databricks.com/clusters/init-scripts.html#cluster-node-initialization-scripts) run during startup for each cluster node before the Spark driver or worker JVM starts
# MAGIC 
# MAGIC Or use [Docker](https://docs.databricks.com/clusters/custom-containers.html) to set up the environment
# MAGIC 
# MAGIC ```
# MAGIC #!/bin/bash
# MAGIC wget https://github.com/arq5x/bedtools2/releases/download/v2.30.0/bedtools.static.binary
# MAGIC mv bedtools.static.binary /opt/bedtools
# MAGIC chmod a+x /opt/bedtools
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,check bedtools was correctly installed across the cluster
# MAGIC %sh
# MAGIC /opt/bedtools

# COMMAND ----------

# DBTITLE 1,Create bed file
bed = spark.createDataFrame([(22, 16050000, 16060000), 
                             (22, 16080000, 16090000), 
                             (22, 16100000, 16110000)], 
                            ("#chrom", "start", "end"))
bed.toPandas().to_csv("/dbfs/tmp/chr22.bed", sep="\t", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### run bedtools on a VCF file with the pipe transformer
# MAGIC 
# MAGIC Here we are going to intersect the vcf with the bed file generated above

# COMMAND ----------

scriptFile = r"""#!/bin/sh
set -e
#input bed is stdin, signified by '-'

/opt/bedtools intersect -seed 42 -a - -b /dbfs/tmp/chr22.bed -header -wa

"""

cmd = ["bash", "-c", scriptFile]

# COMMAND ----------

df_intersect = glow.transform('pipe', 
                               df, 
                               cmd=cmd, 
                               input_formatter='vcf',
                               in_vcf_header='infer',
                               output_formatter='vcf')

# COMMAND ----------

df_intersect.count()

# COMMAND ----------

display(df_intersect)

# COMMAND ----------

dbutils.fs.rm("dbfs:/tmp/chr22.bed")