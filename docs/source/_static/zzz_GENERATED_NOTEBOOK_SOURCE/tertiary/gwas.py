# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Engineering population scale GWAS

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Genome-wide association studies (GWAS) correlate genetic variants with a trait or disease of interest.
# MAGIC 
# MAGIC As cohorts have increased in size to millions, there is a need to robustly engineer GWAS to work at scale.
# MAGIC To that end, we have developed an extensible Spark-native implementation of GWAS using Glow.
# MAGIC 
# MAGIC This notebook leverages the high performance big-data store [Delta Lake](https://delta.io) and uses [mlflow](https://mlflow.org/) to log parameters, metrics and plots associated with each run.

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import pyspark.sql.functions as fx
from pyspark.sql.types import StringType
from pyspark.ml.linalg import Vector, Vectors, SparseVector, DenseMatrix
from pyspark.ml.stat import Summarizer
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.util import MLUtils

from dataclasses import dataclass

import mlflow
import glow
spark = glow.register(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

allele_freq_cutoff = 0.05
num_pcs = 5 #number of principal components
mlflow.log_param("minor allele frequency cutoff", allele_freq_cutoff)
mlflow.log_param("principal components", num_pcs)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paths

# COMMAND ----------

vcf_path = "/databricks-datasets/genomics/1kg-vcfs/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
phenotype_path = "/databricks-datasets/genomics/1000G/phenotypes.normalized"
sample_info_path = "/databricks-datasets/genomics/1000G/samples/populations_1000_genomes_samples.csv"

delta_silver_path = "/mnt/gwas_test/snps.delta"
delta_gold_path = "/mnt/gwas_test/snps.qced.delta"
principal_components_path = "/dbfs/mnt/gwas_test/pcs.csv"
gwas_results_path = "/mnt/gwas_test/gwas_results.delta"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Helper functions

# COMMAND ----------

def plot_layout(plot_title, plot_style, xlabel):
  plt.style.use(plot_style) #e.g. ggplot, seaborn-colorblind, print(plt.style.available)
  plt.title(plot_title)
  plt.xlabel(r'${0}$'.format(xlabel))
  plt.gca().spines['right'].set_visible(False)
  plt.gca().spines['top'].set_visible(False)
  plt.gca().yaxis.set_ticks_position('left')
  plt.gca().xaxis.set_ticks_position('bottom')
  plt.tight_layout()
  
def plot_histogram(df, col, xlabel, xmin, xmax, nbins, plot_title, plot_style, color, vline, out_path):
  plt.close()
  plt.figure()
  bins = np.linspace(xmin, xmax, nbins)
  df = df.toPandas()
  plt.hist(df[col], bins, alpha=1, color=color)
  if vline:
    plt.axvline(x=vline, linestyle='dashed', linewidth=2.0, color='black')
  plot_layout(plot_title, plot_style, xlabel)
  plt.savefig(out_path)
  plt.show()
  
def calculate_pval_bonferroni_cutoff(df, cutoff=0.05):
  bonferroni_p =  cutoff / df.count()
  return bonferroni_p

def get_sample_info(vcf_df, sample_metadata_df):
  """
  get sample IDs from VCF dataframe, index them, then join to sample metadata dataframe
  """
  sample_id_list = vcf_df.limit(1).select("genotypes.sampleId").collect()[0].__getitem__("sampleId")
  sample_id_indexed = spark.createDataFrame(sample_id_list, StringType()). \
                            coalesce(1). \
                            withColumnRenamed("value", "Sample"). \
                            withColumn("index", fx.monotonically_increasing_id())
  sample_id_annotated = sample_id_indexed.join(sample_metadata_df, "Sample")
  return sample_id_annotated

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Ingest 1000 Genomes VCF into Delta Lake
# MAGIC 
# MAGIC Using Glow's VCF reader, which enables variant call format (VCF) files to be read as a Spark Datasource directly from cloud storage,
# MAGIC write genotype data into Delta Lake, a high performance big data store with ACID semantics.
# MAGIC Delta Lake organizes, indexes and compresses data, allowing for performant and reliable computation on genomics data as it grows over time.

# COMMAND ----------

vcf_view_unsplit = (spark.read.format("vcf")
   .option("flattenInfoFields", "false")
   .load(vcf_path))

# COMMAND ----------

# MAGIC %md Split multiallelics variants to biallelics

# COMMAND ----------

vcf_view = glow.transform("split_multiallelics", vcf_view_unsplit)

# COMMAND ----------

display(vcf_view.withColumn("genotypes", fx.col("genotypes")[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: we compute variant-wise summary stats and Hardy-Weinberg equilibrium P values using `call_summary_stats` & `hardy_weinberg`, which are built into Glow

# COMMAND ----------

(vcf_view
  .select(
    fx.expr("*"),
    glow.expand_struct(glow.call_summary_stats(fx.col("genotypes"))),
    glow.expand_struct(glow.hardy_weinberg(fx.col("genotypes")))
  )
  .write
  .mode("overwrite")
  .format("delta")
  .save(delta_silver_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Since metadata associated with Delta Lake are stored directly in the transaction log, we can quickly calculate the size of the Delta Lake and log it to MLflow

# COMMAND ----------

num_variants = spark.read.format("delta").load(delta_silver_path).count()
mlflow.log_metric("Number Variants pre-QC", num_variants)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Run Quality Control
# MAGIC 
# MAGIC Perform variant-wise filtering on Hardy-Weinberg equilibrium P-values and allele frequency

# COMMAND ----------

hwe = (spark.read.format("delta")
                 .load(delta_silver_path)
                 .where((fx.col("alleleFrequencies").getItem(0) >= allele_freq_cutoff) & 
                       (fx.col("alleleFrequencies").getItem(0) <= (1.0 - allele_freq_cutoff)))
                 .withColumn("log10pValueHwe", fx.when(fx.col("pValueHwe") == 0, 26).otherwise(-fx.log10(fx.col("pValueHwe")))))

# COMMAND ----------

hwe_cutoff = calculate_pval_bonferroni_cutoff(hwe)
mlflow.log_param("Hardy-Weinberg P value cutoff", hwe_cutoff)

# COMMAND ----------

display(plot_histogram(df=hwe.select("log10pValueHwe"), 
                       col="log10pValueHwe",
                       xlabel='-log_{10}(P)',
                       xmin=0, 
                       xmax=25, 
                       nbins=50, 
                       plot_title="hardy-weinberg equilibrium", 
                       plot_style="ggplot",
                       color='#e41a1c',
                       vline = -np.log10(hwe_cutoff),
                       out_path = "/databricks/driver/hwe.png"
                      )
       )

# COMMAND ----------

mlflow.log_artifact("/databricks/driver/hwe.png")

# COMMAND ----------

(spark.read.format("delta")
   .load(delta_silver_path)
   .where((fx.col("alleleFrequencies").getItem(0) >= allele_freq_cutoff) & 
         (fx.col("alleleFrequencies").getItem(0) <= (1.0 - allele_freq_cutoff)) &
         (fx.col("pValueHwe") >= hwe_cutoff))
   .write
   .mode("overwrite")
   .format("delta")
   .save(delta_gold_path))

# COMMAND ----------

num_variants = spark.read.format("delta").load(delta_gold_path).count()
mlflow.log_metric("Number Variants post-QC", num_variants)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Run Principal Component Analysis (PCA)
# MAGIC 
# MAGIC To control for ancestry in the GWAS
# MAGIC 
# MAGIC Note: `array_to_sparse_vector` is a function built into Glow

# COMMAND ----------

vectorized = (spark.read.format("delta")
                        .load(delta_gold_path)
                        .select(glow.array_to_sparse_vector(glow.genotype_states(fx.col("genotypes"))).alias("features"))
                        .cache())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Use `pyspark.ml` to calculate principal components on sparse vector

# COMMAND ----------

matrix = RowMatrix(MLUtils.convertVectorColumnsFromML(vectorized, "features").rdd.map(lambda x: x.features))
pcs = matrix.computeSVD(num_pcs)

# COMMAND ----------

pd.DataFrame(pcs.V.toArray()).to_csv(principal_components_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read sample information in and plot out principal components
# MAGIC 
# MAGIC Here we annotate sample info with principal components by joining both DataFrames on index
# MAGIC 
# MAGIC Note: indexing is performed using the Spark SQL function `monotonically_increasing_id()`

# COMMAND ----------

pcs_df = spark.createDataFrame(pcs.V.toArray().tolist(), ["pc" + str(i) for i in range(num_pcs)])

# COMMAND ----------

sample_metadata = spark.read.option("header", True).csv(sample_info_path)
sample_info = get_sample_info(vcf_view, sample_metadata)
sample_count = sample_info.count()
mlflow.log_param("number of samples", sample_count)
pcs_indexed = pcs_df.coalesce(1).withColumn("index", fx.monotonically_increasing_id())
pcs_with_samples = pcs_indexed.join(sample_info, "index")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Use the display function to create a scatter plot of pc1 and pc2
# MAGIC 
# MAGIC Note: because we are only analyzing chromosome 22
# MAGIC 
# MAGIC the PCA scatter plot does not distinguish populations as well as the whole genome data

# COMMAND ----------

display(pcs_with_samples)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Prepare data for GWAS

# COMMAND ----------

phenotype_df = pd.read_parquet('/dbfs/' + phenotype_path).explode('values').rename({'values': 'bmi'}, axis='columns').reset_index(drop=True)
del phenotype_df['phenotype']
phenotype_df

# COMMAND ----------

covariate_df = pd.read_csv(principal_components_path)

# COMMAND ----------

phenotype = phenotype_df.columns[0]
mlflow.log_param("phenotype", phenotype)

# COMMAND ----------

genotypes = spark.read.format("delta").load(delta_gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run `linear_regression`
# MAGIC 
# MAGIC Note: `genotype_states` is a utility function in Glow that converts an genotypes array, e.g. `[0,1]` into an integer containing the number of alternate alleles, e.g. `1`

# COMMAND ----------

results = glow.gwas.linear_regression(
  genotypes.select('contigName', 'start', 'names', 'genotypes'),
  phenotype_df,
  covariate_df,
  values_column=glow.genotype_states(fx.col('genotypes'))
)

(results.write
  .format("delta")
  .mode("overwrite")
  .save(gwas_results_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Show results

# COMMAND ----------

display(spark.read.format("delta").load(gwas_results_path).limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load GWAS results into R and plot using `qqman` library

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC gwas_df <- read.df("/mnt/gwas_test/gwas_results.delta", source="delta")
# MAGIC gwas_results <- select(gwas_df, c(cast(alias(gwas_df$contigName, "CHR"), "double"), alias(gwas_df$start, "BP"), alias(gwas_df$pValue, "P"), alias(element_at(gwas_df$names, 1L), "SNP")))
# MAGIC gwas_results_rdf <- as.data.frame(gwas_results)

# COMMAND ----------

# MAGIC %r
# MAGIC install.packages("qqman", repos="http://cran.us.r-project.org")
# MAGIC library(qqman)

# COMMAND ----------

# MAGIC %r
# MAGIC png('/databricks/driver/manhattan.png')
# MAGIC manhattan(gwas_results_rdf, 
# MAGIC           col = c("#228b22", "#6441A5"), 
# MAGIC           chrlabs = NULL,
# MAGIC           suggestiveline = -log10(1e-05), 
# MAGIC           genomewideline = -log10(5e-08),
# MAGIC           highlight = NULL, 
# MAGIC           logp = TRUE, 
# MAGIC           annotatePval = NULL, 
# MAGIC           ylim=c(0,17))
# MAGIC dev.off()

# COMMAND ----------

# MAGIC %r
# MAGIC manhattan(gwas_results_rdf, col = c("#228b22", "#6441A5"), chrlabs = NULL,
# MAGIC suggestiveline = -log10(1e-05), genomewideline = -log10(5e-08),
# MAGIC highlight = NULL, logp = TRUE, annotatePval = NULL, ylim=c(0,17))

# COMMAND ----------

mlflow.log_artifact('/databricks/driver/manhattan.png')

# COMMAND ----------

# MAGIC %r
# MAGIC png('/databricks/driver/qqplot.png')
# MAGIC qq(gwas_results_rdf$P)
# MAGIC dev.off()

# COMMAND ----------

# MAGIC %r
# MAGIC qq(gwas_results_rdf$P)

# COMMAND ----------

mlflow.log_artifact('/databricks/driver/qqplot.png')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean up

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/gwas_test", True)