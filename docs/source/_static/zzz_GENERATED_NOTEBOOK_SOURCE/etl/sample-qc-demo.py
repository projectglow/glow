# Databricks notebook source
import glow
spark = glow.register(spark)
from pyspark.sql.functions import *
path = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"

# COMMAND ----------

df = spark.read.format("vcf").option("includeSampleIds", True).load(path)

# COMMAND ----------

# DBTITLE 1,Build array of QC stats
qc = df.selectExpr("sample_call_summary_stats(genotypes, referenceAllele, alternateAlleles) as qc").cache()

# COMMAND ----------

# DBTITLE 1,Show per-sample QC metrics
display(qc.selectExpr("explode(qc) as per_sample_qc").selectExpr("expand_struct(per_sample_qc)"))

# COMMAND ----------

# DBTITLE 1,Filter input DataFrame by QC metrics
filtered = (df
  .crossJoin(qc)
  .selectExpr("filter(genotypes, (g, i) -> qc[i].rHetHomVar < 2) as filtered_genotypes"))

# COMMAND ----------

# DBTITLE 1,Size of genotypes array before filtering
display(df.selectExpr("size(genotypes)"))

# COMMAND ----------

# DBTITLE 1,Size of genotypes array after filtering -- some samples were removed!
display(filtered.selectExpr("size(filtered_genotypes)"))

# COMMAND ----------

# DBTITLE 1,Explode genotype array and use built in Spark aggregations
exploded_df = (df.withColumn("genotype", explode("genotypes"))
  .withColumn("hasNonRef", expr("exists(genotype.calls, call -> call != -1 and call != 0)")))
display(exploded_df
        .groupBy("genotype.sampleId", "INFO_SVTYPE", "hasNonRef")
        .agg(count(lit(1)))
        .orderBy("sampleId", "INFO_SVTYPE", "hasNonRef"))