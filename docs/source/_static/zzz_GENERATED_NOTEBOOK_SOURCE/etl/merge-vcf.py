# Databricks notebook source
from pyspark.sql.functions import *
import glow
glow.register(spark)

# COMMAND ----------

# DBTITLE 1,Split Thousand Genome Project multi-sample VCF into 2 single-sample VCFs
vcf_df = spark.read.format('vcf').load('/databricks-datasets/genomics/1kg-vcfs/*.vcf.gz')
vcf_split1 = vcf_df.withColumn('genotypes', expr('filter(genotypes, (g, idx) -> g.sampleId = genotypes[0].sampleId)'))
vcf_split2 = vcf_df.withColumn('genotypes', expr('filter(genotypes, (g, idx) -> g.sampleId = genotypes[1].sampleId)'))
vcf_split1.write.format('bigvcf').mode('overwrite').save('/tmp/vcf-merge-demo/1.vcf.bgz')
vcf_split2.write.format('bigvcf').mode('overwrite').save('/tmp/vcf-merge-demo/2.vcf.bgz')

# COMMAND ----------

# DBTITLE 1,Show contents before merge
df_to_merge = spark.read.format('vcf').load(['/hhd/vcf-merge-demo/1.vcf.bgz', '/hhd/vcf-merge-demo/2.vcf.bgz'])
display(df_to_merge.select('contigName', 'start', col('genotypes').sampleId).orderBy('contigName', 'start', 'genotypes.sampleId'))

# COMMAND ----------

# DBTITLE 1,Merge genotype arrays
merged = df_to_merge.groupBy('contigName', 'start', 'end', 'referenceAllele', 'alternateAlleles')\
  .agg(sort_array(flatten(collect_list('genotypes'))).alias('genotypes'))
display(merged.orderBy('contigName', 'start').select('contigName', 'start', col('genotypes').sampleId))

# COMMAND ----------

# DBTITLE 1,Merge VCFs and sum INFO_DP
merged = df_to_merge.groupBy('contigName', 'start', 'end', 'referenceAllele', 'alternateAlleles')\
  .agg(sort_array(flatten(collect_list('genotypes'))).alias('genotypes'), sum('INFO_DP').alias('INFO_DP'))
display(merged.orderBy('contigName', 'start').select('contigName', 'start', 'INFO_DP', col('genotypes').sampleId))