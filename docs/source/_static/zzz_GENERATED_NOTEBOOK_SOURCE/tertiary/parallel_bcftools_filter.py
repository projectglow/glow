# Databricks notebook source
# MAGIC %md
# MAGIC ###<img src="https://databricks.com/wp-content/themes/databricks/assets/images/header_logo_2x.png" alt="logo" width="150"/> 
# MAGIC 
# MAGIC ### Parallel bcftools filter of vcf files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment setup
# MAGIC 
# MAGIC The [Glow Docker Container](https://github.com/projectglow/glow/blob/master/docker/databricks/dbr/dbr10.4/genomics/Dockerfile) contains bcftools preinstalled

# COMMAND ----------

import pyspark.sql.functions as fx
import subprocess

# COMMAND ----------

'''
This will set a spark property AQE to false. This property would otherwise automatically put place many VCF files' processing together under the same core since the raw data in that df dataframe is small (the dataframe is being used to orchestrating tasks, with each row defining parameters for each task) . This is not a desirable result in our specific case. Usually, this property works in favor but not here.
'''
spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

import time
start_time = time.time()

import pytz
from datetime import datetime
tz = pytz.timezone('US/Pacific')
init_time = datetime.now(tz)
print(init_time.strftime('%Y-%m-%d %H:%M:%S'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe creation

# COMMAND ----------

# MAGIC %md
# MAGIC ##### set up paths, note: please change

# COMMAND ----------

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
path_to_vcf = "dbfs:/databricks-datasets/genomics/jg-sample/"
outpath = "dbfs:/tmp/" + user + "/test/filtered_vcf/"
outpath_local = "/dbfs/tmp/" + user + "/test/filtered_vcf/"
dp_filter = 10
qual_filter = 25
num_files = 2

# COMMAND ----------

dbutils.fs.rm(outpath, True)
dbutils.fs.mkdirs(outpath)

# COMMAND ----------

df_rdd = sc.parallelize(dbutils.fs.ls(path_to_vcf), num_files)
df = df_rdd.toDF()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC use local file system API to set up paths for input and output files

# COMMAND ----------

df1 = df.filter(fx.col("name").rlike(".vcf.bgz")). \
        withColumn('path', fx.regexp_replace('path', 'dbfs:', '/dbfs')). \
        withColumn('outpath', fx.regexp_replace('name', '.vcf.gz', '.filtered.vcf.gz')). \
        withColumn('vcf_outpath', fx.concat(fx.lit(outpath_local), fx.col('outpath'))). \
        withColumn('dpf', fx.lit(dp_filter)). \
        withColumn('qualf', fx.lit(qual_filter)). \
        drop("outpath")
display(df1)

# COMMAND ----------

def bcftools_filter_cmd(vcf_path, vcf_outpath, dp_filter, qual_filter):
        """
        python wrapper to run bcftools filter from command line in parallel
        """
        cmd = ('''/opt/bcftools-1.15.1/bcftools filter -e '(FORMAT/DP<{dp_filter}&QUAL=".")|(QUAL>=0&GT="0|0")|(QUAL>=0&GT="0/0")|QUAL<{qual_filter}' -O z -o {vcf_outpath} {vcf_path} && /opt/htslib-1.15.1/tabix -f {vcf_outpath} ''').format(vcf_path=vcf_path, vcf_outpath=vcf_outpath, dp_filter=dp_filter, qual_filter=qual_filter)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        s_output, s_err = proc.communicate()
        s_return =  proc.returncode
        assert s_return == 0, 'Could not filter vcf files' + str(s_output) + '\n' + str(s_err)
        return s_return, s_output, s_err

# COMMAND ----------

df1.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC use rdd.map to process each vcf on individual spark workers one at a time

# COMMAND ----------

df2 = df1.rdd.map(lambda row: bcftools_filter_cmd(str(row[0]), str(row[4]), str(row[5]), str(row[6])))

# COMMAND ----------

# MAGIC %md
# MAGIC since Spark executes lazily, run a count on the df to kick off the parallel bgzip

# COMMAND ----------

df2.count()

# COMMAND ----------

# DBTITLE 1,Time taken to run the entire code
print(str(round((datetime.now(tz) - init_time).seconds/60, 2)), " minutes")

# COMMAND ----------

# DBTITLE 1,Current time (in Pacific time zone)
print(datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### compare file sizes before and after filtering

# COMMAND ----------

display(dbutils.fs.ls(path_to_vcf))

# COMMAND ----------

display(dbutils.fs.ls(outpath))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### restore spark configurations back to default

# COMMAND ----------

spark.sql("RESET")