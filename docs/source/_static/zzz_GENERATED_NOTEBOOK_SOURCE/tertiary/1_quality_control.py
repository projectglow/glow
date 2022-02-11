# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Quality control to prepare delta table for GWAS
# MAGIC 
# MAGIC By running glow transform functions `split_multiallelics`, `mean_substitute`, and `genotype_states`
# MAGIC 
# MAGIC Important: please checkpoint to parquet/delta after each step in this process
# MAGIC 
# MAGIC Then filter,
# MAGIC 
# MAGIC 1. monomorphic variants using `array_distinct`
# MAGIC 2. allele frequency
# MAGIC 3. hardy weinberg equilibrium

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %run ../2_setup_metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ##### adjust spark confs
# MAGIC 
# MAGIC see [split-multiallelics](https://glow.readthedocs.io/en/latest/etl/variant-splitter.html#split-multiallelics) docs

# COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define QC steps

# COMMAND ----------

method = 'quality_control'
step1 = 'split_multiallelics'
step2 = 'left_normalize_indels'
step3 = 'mean_substitute'
step4 = 'call_summary_stats'
step5 = 'variant_filter'
library = 'glow'
datetime = datetime.now(pytz.timezone('US/Pacific'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Helper functions

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

# COMMAND ----------

#delta lake generates paths on the fly for objects in cloud storage, 
#but for local files synced to cloud storage we need to create a path
dbutils.fs.mkdirs(output_hwe_path) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### split simulated data
# MAGIC 
# MAGIC 1. biallelic SNPs
# MAGIC 2. multiallelic variants
# MAGIC 3. indels

# COMMAND ----------

delta_vcf = spark.read.format("delta").load(output_simulated_delta)

# COMMAND ----------

display(delta_vcf.drop("genotypes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### split multiallelics
# MAGIC 
# MAGIC write out biallelics and split multiallelics

# COMMAND ----------

start_time = time.time()
multiallelic_df = delta_vcf.where(fx.size(fx.col("alternateAlleles")) > 1)
multiallelic_df = glow.transform('split_multiallelics', multiallelic_df)
bialleleic_df = delta_vcf.where(fx.size(fx.col("alternateAlleles")) == 1)

multiallelic_df.write.mode("overwrite").format("delta").save(output_delta_split_multiallelics)
bialleleic_df.write.mode("append").format("delta").save(output_delta_split_multiallelics)

end_time = time.time()
log_metadata(datetime, n_samples, n_variants, 0, 0, 'etl', step1, library, spark_version, node_type_id, n_workers, start_time, end_time, run_metadata_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### extract indels and left-normalize

# COMMAND ----------

start_time = time.time()
split_multiallelic_df = spark.read.format("delta").load(output_delta_split_multiallelics)
indels_df = split_multiallelic_df.where((fx.length("referenceAllele") > 1) | (fx.length(fx.col("alternateAlleles")[0]) > 1))
snps_df = split_multiallelic_df.where((fx.length("referenceAllele") == 1) & (fx.length(fx.col("alternateAlleles")[0]) == 1))

normalized_variants_df = glow.transform(
  "normalize_variants",
  indels_df,
  reference_genome_path=reference_genome_path
)

num_variants_changed = normalized_variants_df.where(fx.col("normalizationStatus.changed") == True).count()

print("number of variants left normalized = " + str(num_variants_changed))

snps_df.write.mode("overwrite").format("delta").save(output_delta_split_multiallelics_normalize)
normalized_variants_df.drop("normalizationStatus"). \
                       write.mode("append").format("delta"). \
                       save(output_delta_split_multiallelics_normalize)

end_time = time.time()
log_metadata(datetime, n_samples, n_variants, 0, 0, 'etl', step2, library, spark_version, node_type_id, n_workers, start_time, end_time, run_metadata_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### prepare simulated delta table for GWAS using glow transformers

# COMMAND ----------

start_time = time.time()
delta_vcf = spark.read.format("delta").load(output_delta_split_multiallelics_normalize)
delta_gwas_vcf = (delta_vcf.withColumn('values', glow.mean_substitute(glow.genotype_states('genotypes'))). \
                  filter(fx.size(fx.array_distinct('values')) > 1)
                 )
delta_gwas_vcf.write.mode("overwrite").format("delta").save(output_delta_glow_qc_transformers)
end_time = time.time()
log_metadata(datetime, n_samples, n_variants, 0, 0, 'etl', step3, library, spark_version, node_type_id, n_workers, start_time, end_time, run_metadata_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### perform variant-level quality control using glow helper functions

# COMMAND ----------

start_time = time.time()
delta_gwas_vcf = spark.read.format("delta").load(output_delta_glow_qc_transformers)

summary_stats_df = delta_gwas_vcf.select(
    fx.expr("*"),
    glow.expand_struct(glow.call_summary_stats(fx.col("genotypes"))),
    glow.expand_struct(glow.hardy_weinberg(fx.col("genotypes")))
  ). \
    withColumn("log10pValueHwe", fx.when(fx.col("pValueHwe") == 0, 26).otherwise(-fx.log10(fx.col("pValueHwe"))))
summary_stats_df.drop("genotypes").write.mode("overwrite").format("delta").save(output_delta_glow_qc_variants)

end_time = time.time()
log_metadata(datetime, n_samples, n_variants, 0, 0, method, step4, library, spark_version, node_type_id, n_workers, start_time, end_time, run_metadata_delta_path)

# COMMAND ----------

display(summary_stats_df.drop("genotypes", "values"))

# COMMAND ----------

hwe_cutoff = calculate_pval_bonferroni_cutoff(summary_stats_df)

# COMMAND ----------

display(plot_histogram(df=summary_stats_df.select("log10pValueHwe"), 
                       col="log10pValueHwe",
                       xlabel='-log_{10}(P)',
                       xmin=0, 
                       xmax=25, 
                       nbins=50, 
                       plot_title="hardy-weinberg equilibrium", 
                       plot_style="ggplot",
                       color='#e41a1c',
                       vline = -np.log10(hwe_cutoff),
                       out_path = output_hwe_plot
                      )
       )

# COMMAND ----------

start_time = time.time()
variant_filter_df = spark.read.format("delta").load(output_delta_glow_qc_variants)

variant_filter_df = summary_stats_df.where((fx.col("alleleFrequencies").getItem(0) >= allele_freq_cutoff) & 
                                           (fx.col("alleleFrequencies").getItem(0) <= (1.0 - allele_freq_cutoff)) &
                                           (fx.col("pValueHwe") >= hwe_cutoff)
                                          )

variant_filter_df.write.option("overwriteSchema", "true").mode("overwrite").format("delta").save(output_delta_transformed)

end_time = time.time()
log_metadata(datetime, n_samples, n_variants, 0, 0, method, step5, library, spark_version, node_type_id, n_workers, start_time, end_time, run_metadata_delta_path)

# COMMAND ----------

spark.read.format('delta').load(output_delta_transformed).count()