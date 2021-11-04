# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Quality control to prepare delta table for GWAS
# MAGIC 
# MAGIC By running glow transform functions `split_multiallelics`, `mean_substitute`, and `genotype_states`
# MAGIC 
# MAGIC Then filter,
# MAGIC 
# MAGIC 1. monomorphic variants using `array_distinct`
# MAGIC 2. allele frequency
# MAGIC 3. hardy weinberg equilibrium

# COMMAND ----------

# MAGIC %md
# MAGIC ##### adjust spark confs
# MAGIC 
# MAGIC see [split-multiallelics](https://glow.readthedocs.io/en/latest/etl/variant-splitter.html#split-multiallelics) docs

# COMMAND ----------

spark.conf.set("spark.sql.codegen.wholeStage", False)

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants

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
# MAGIC ##### prepare simulated delta table for GWAS

# COMMAND ----------

delta_vcf = spark.read.format("delta").load(output_delta)
delta_gwas_vcf = (glow.transform('split_multiallelics', delta_vcf). \
                  withColumn('values', glow.mean_substitute(glow.genotype_states('genotypes'))). \
                  filter(fx.size(fx.array_distinct('values')) > 1)
                 )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### perform quality control using glow helper functions

# COMMAND ----------

summary_stats_df = delta_gwas_vcf.select(
    fx.expr("*"),
    glow.expand_struct(glow.call_summary_stats(fx.col("genotypes"))),
    glow.expand_struct(glow.hardy_weinberg(fx.col("genotypes")))
  ). \
    withColumn("log10pValueHwe", fx.when(fx.col("pValueHwe") == 0, 26).otherwise(-fx.log10(fx.col("pValueHwe"))))

# COMMAND ----------

display(summary_stats_df.drop("genotypes", "values"))

# COMMAND ----------

hwe_cutoff = calculate_pval_bonferroni_cutoff(summary_stats_df)
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

variant_filter_df = summary_stats_df.where((fx.col("alleleFrequencies").getItem(0) >= allele_freq_cutoff) & 
                                           (fx.col("alleleFrequencies").getItem(0) <= (1.0 - allele_freq_cutoff)) &
                                           (fx.col("pValueHwe") >= hwe_cutoff)
                                          )

# COMMAND ----------

variant_filter_df.write.option("overwriteSchema", "true").mode("overwrite").format("delta").save(output_delta_transformed)

# COMMAND ----------

spark.read.format('delta').load(output_delta_transformed).count()