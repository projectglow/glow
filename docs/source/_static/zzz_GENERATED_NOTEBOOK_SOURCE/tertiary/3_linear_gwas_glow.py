# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://databricks.com/wp-content/themes/databricks/assets/images/databricks-logo.png" alt="logo" width="240"/> + <img src="https://www.regeneron.com/Content/images/science/regenron.png" alt="logo" width="240"/>
# MAGIC 
# MAGIC ### <img src="https://databricks-knowledge-repo-images.s3.us-east-2.amazonaws.com/HLS/glow/project_glow_logo.png" alt="logo" width="35"/> Linear regression genetic association study
# MAGIC 
# MAGIC Uses a simulated project-level VCF from chromosomes 21 and 22 of the 1000 genomes

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 100)

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

method = 'linear'
test = 'linear_regression'
library = 'glow'
datetime = datetime.now(pytz.timezone('US/Pacific'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### load genotypes

# COMMAND ----------

delta_vcf = spark.read.format("delta").load(variants_path)
n_filtered_variants = delta_vcf.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### load simulated covariates, phentoypes, and offsets

# COMMAND ----------

covariates = pd.read_csv(covariates_path, dtype={'sample_id': str}, index_col='sample_id')
covariates.head(5)

# COMMAND ----------

phenotypes = pd.read_csv(quantitative_phenotypes_path, dtype={'sample_id': str}, index_col='sample_id')
phenotypes.head(5)

# COMMAND ----------

offsets = pd.read_csv(quantitative_y_hat_path, dtype={'sample_id': str}).set_index(['sample_id'])
offsets.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### run linear regression gwas

# COMMAND ----------

start_time_linreg = time.time()

for num, contig in enumerate(contigs):
  offsets_chr = offsets[offsets['contigName'] == contig].drop(['contigName'], axis=1) 
  results = glow.gwas.linear_regression(
    delta_vcf.where(fx.col('contigName') == contig),
    phenotypes,
    covariates,
    offsets_chr,
    values_column='values',
    # In addition to filtering the DataFrame, hint to Glow that the input only contains one contig
    contigs=[contig])
  
  results.write.format('delta'). \
                mode('append'). \
                save(linear_gwas_results_path_confounded)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### log runtime

# COMMAND ----------

end_time_linreg = time.time()
log_metadata(datetime, n_samples, n_filtered_variants, n_covariates, n_quantitative_phenotypes, method, test, library, spark_version, node_type_id, n_workers, start_time_linreg, end_time_linreg, run_metadata_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Plot results using `R` package `qqman`

# COMMAND ----------

results_df = spark.read.format("delta").load(linear_gwas_results_path_confounded). \
                                        withColumn("log_p", -fx.log10("pvalue")).cache()
display(results_df.orderBy("pvalue"))

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC gwas_df <- read.df('dbfs:/home/william.brandler@databricks.com/genomics/data/delta/simulate_pvcf_linear_gwas_results_confounded.delta', source="delta")
# MAGIC gwas_results <- select(gwas_df, c(alias(element_at(gwas_df$names, as.integer(1)), "SNP"), 
# MAGIC                                   cast(alias(gwas_df$contigName, "CHR"), "double"), 
# MAGIC                                   alias(gwas_df$start, "BP"), alias(gwas_df$pvalue, "P")))
# MAGIC gwas_results_rdf <- as.data.frame(gwas_results)
# MAGIC gwas_results_rdf

# COMMAND ----------

# MAGIC %r
# MAGIC install.packages("qqman", repos="http://cran.us.r-project.org")
# MAGIC library(qqman)

# COMMAND ----------

# MAGIC %r
# MAGIC qq(gwas_results_rdf$P)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Confounded!
# MAGIC 
# MAGIC Looks like there's inflation in the P values. 
# MAGIC 
# MAGIC This data has a lot of missing phenotypes, which are mean imputed. Perhaps this is causing the inflation?
# MAGIC 
# MAGIC Let's go ahead and filter out the samples with missing phenotypes...

# COMMAND ----------

# MAGIC %md
# MAGIC ##### define functions to filter phenotypes and genotypes

# COMMAND ----------

from typing import List

def filter_phenotypes(phenotypes_pdf):
  filtered_phenotypes_pdf = phenotypes_pdf[phenotypes_pdf['QP1'].notnull()]
  return filtered_phenotypes_pdf

def extract_sample_ids_without_missing_phenotypes(phenotypes_df, phenotype_str):
  """
  get a list of sample ids that do not have missing phenotypes for a given phenotype
  return sample ids are a list of strings
  """
  phenotypes_df['sample_id'] = phenotypes_df.index
  sample_ids = phenotypes_df[phenotypes_df[phenotype_str].notnull()]['sample_id'].tolist()
  sample_ids: Optional[List[str]] = [str(x) for x in sample_ids]
  return sample_ids

def filter_genotypes(genotypes_df, list_sample_ids):
  """
  given a python list of sample ids, 
  return a filtered_genotypes dataframe with samples from that list
  """
  sample_subset='\' , \''.join(list_sample_ids)
  filtered_genotypes=genotypes_df.selectExpr("*", "filter(genotypes, g -> array_contains(array('{0}'), g.sampleId)) as genotypes2".format(sample_subset)).\
                                  drop("genotypes").\
                                  withColumnRenamed("genotypes2", "genotypes")
  return filtered_genotypes

# COMMAND ----------

# MAGIC %md
# MAGIC ##### filter genotypes
# MAGIC 
# MAGIC based on samples with missing phenotypes

# COMMAND ----------

filtered_phenotypes_pdf = filter_phenotypes(phenotypes)
sample_ids_to_keep = extract_sample_ids_without_missing_phenotypes(phenotypes, "QP1")
print("keeping " + str(len(sample_ids_to_keep)) + " samples after filtering those with missing phenotypic data")

# COMMAND ----------

start_time_filter = time.time()
filtered_genotypes_df = filter_genotypes(delta_vcf, sample_ids_to_keep)
filtered_genotypes_df.write.mode("overwrite").format("delta").save(qc_samples_path)
end_time_filter = time.time()
log_metadata(datetime, n_samples, n_variants, 0, 0, "quality_control", "sample_filter", library, spark_version, node_type_id, n_workers, start_time_filter, end_time_filter, run_metadata_delta_path)

# COMMAND ----------

filtered_genotypes_df = spark.read.format("delta").load(qc_samples_path)
filtered_genotypes_df = filtered_genotypes_df.withColumn('values', glow.mean_substitute(glow.genotype_states('genotypes'))) #prepare for gwas
filtered_genotypes_df.select(fx.size("values")).limit(5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### rerun regression

# COMMAND ----------

dbutils.fs.rm(linear_gwas_results_path, recurse=True)

for num, contig in enumerate(contigs):
  offsets_chr = offsets[offsets['contigName'] == contig].drop(['contigName'], axis=1) 
  results = glow.gwas.linear_regression(
    filtered_genotypes_df.where(fx.col('contigName') == contig),
    filtered_phenotypes_pdf,
    covariates,
    offsets_chr,
    values_column='values',
    contigs=[contig],
    intersect_samples=True,
    genotype_sample_ids=sample_ids_to_keep
  )
  
  results.write.format('delta'). \
                mode('append'). \
                save(linear_gwas_results_path)

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC gwas_df <- read.df('dbfs:/home/william.brandler@databricks.com/genomics/data/delta/simulate_pvcf_linear_gwas_results.delta', source="delta")
# MAGIC gwas_results <- select(gwas_df, c(alias(element_at(gwas_df$names, as.integer(1)), "SNP"), 
# MAGIC                                   cast(alias(gwas_df$contigName, "CHR"), "double"), 
# MAGIC                                   alias(gwas_df$start, "BP"), alias(gwas_df$pvalue, "P")))
# MAGIC gwas_results_rdf <- as.data.frame(gwas_results)

# COMMAND ----------

# MAGIC %r
# MAGIC qq(gwas_results_rdf$P)

# COMMAND ----------

# MAGIC %r
# MAGIC manhattan(gwas_results_rdf, 
# MAGIC           col = c("#228b22", "#6441A5"), 
# MAGIC           chrlabs = NULL,
# MAGIC           suggestiveline = -log10(1e-05), 
# MAGIC           genomewideline = -log10(5e-08),
# MAGIC           highlight = NULL, 
# MAGIC           logp = TRUE, 
# MAGIC           annotatePval = NULL, 
# MAGIC           ylim=c(0,9))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Thank you for completing the tutorial!
# MAGIC 
# MAGIC Please explore more in the [Glow Documentation](https://glow.readthedocs.io/en/latest/)