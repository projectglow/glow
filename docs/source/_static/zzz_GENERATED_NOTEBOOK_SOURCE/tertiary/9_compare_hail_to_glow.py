# Databricks notebook source
# MAGIC %md
# MAGIC ##### Read Hail results dataframe, convert to Spark, and compare to Glow results

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../1_setup_constants_hail

# COMMAND ----------

# MAGIC %md
# MAGIC ##### define functions

# COMMAND ----------

def annotate_hl_result_table_linear(result_table):
  result_table  = result_table.annotate(z_results=hl.zip(result_table["phenotypes"],
                                                         result_table["p_value"],
                                                         result_table["beta"],
                                                         result_table["y_transpose_x"],
                                                         result_table["standard_error"]))
  
  result_table = result_table.explode("z_results")
  
  result_table = result_table.annotate(
    phenotype=result_table["z_results"][0],
    n=result_table["n"],
    sum_x=result_table["sum_x"],
    p_value=result_table["z_results"][1],
    beta=result_table["z_results"][2],      
    y_transpose_x=result_table["z_results"][3],
    standard_error=result_table["z_results"][4]
  ).drop("z_results")
  
  return result_table

def select_cols_hail_gwas_results_linear(hl_result_df):
  """
  rename hail columns for linear regression to match glow
  """
  hl_result_df = hl_result_df.select(hl_result_df["phenotype"],hl_result_df["`locus.contig`"].alias("contigName"),
                                     (hl_result_df["`locus.position`"] - fx.lit(1)).alias("start"),
                                     hl_result_df.alleles[0].alias("referenceAllele"),
                                     fx.array(hl_result_df.alleles[1]).alias("alternateAlleles"),
                                     hl_result_df.p_value.alias("hl_p_value"), hl_result_df.beta.alias("hl_beta"), 
                                     hl_result_df.n.alias("hl_n"),
                                     hl_result_df.sum_x.alias("hl_sum_x"), 
                                     hl_result_df.y_transpose_x.alias("hl_y_transpose_x"),
                                     hl_result_df.standard_error.alias("hl_standard_error")
                                    )
  return hl_result_df

def explode_ht_to_df_linear(result_table, phenotypes):
  """
  take hail linear regression results, match to glow, convert to Spark dataframe
  """
  result_table = result_table.annotate_globals(phenotypes=phenotypes)
  result_table = annotate_hl_result_table_linear(result_table)
  hl_result_df = result_table.to_spark()
  hl_result_df = select_cols_hail_gwas_results_linear(hl_result_df)
  return hl_result_df

def annotate_hl_result_table_logistic(result_table):
  result_table  = result_table.annotate(z_results=hl.zip(result_table["phenotypes"],
                                                         result_table["logistic_regression"]["p_value"],
                                                         result_table["logistic_regression"]["beta"],
                                                         result_table["logistic_regression"]["chi_sq_stat"]))
  
  result_table = result_table.explode("z_results")
  
  result_table = result_table.annotate(
    phenotype=result_table["z_results"][0],
    p_value=result_table["z_results"][1],
    beta=result_table["z_results"][2],      
    chi_sq_stat=result_table["z_results"][3]
  ).drop("z_results")
  
  return result_table

def select_cols_hail_gwas_results_logistic(hl_result_df):
  """
  rename hail columns for logistic regression to match glow
  """
  hl_result_df = hl_result_df.select(hl_result_df["phenotype"], hl_result_df["`locus.contig`"].alias("contigName"),
                                     (hl_result_df["`locus.position`"] - fx.lit(1)).alias("start"),
                                     hl_result_df.alleles[0].alias("referenceAllele"),
                                     fx.array(hl_result_df.alleles[1]).alias("alternateAlleles"),
                                     hl_result_df.p_value.alias("hl_p_value"), hl_result_df.beta.alias("hl_beta"), 
                                     hl_result_df.chi_sq_stat.alias("hl_chi_sq_stat")
                                    )
  return hl_result_df

def explode_ht_to_df_logistic(result_table, phenotypes):
  """
  take hail logistic regression results, match to glow, convert to Spark dataframe
  """
  result_table = result_table.annotate_globals(phenotypes=phenotypes)
  result_table = annotate_hl_result_table_logistic(result_table)
  hl_result_df = result_table.to_spark()
  hl_result_df = select_cols_hail_gwas_results_logistic(hl_result_df)
  return hl_result_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load linear regression data

# COMMAND ----------

quantitative_phenotype_df = spark.createDataFrame(pd.read_csv(quantitative_phenotypes_path).set_index("sample_id", drop=False)). \
                                  withColumn("sample_id", fx.col("sample_id").cast(StringType()))
quantitative_phenotypes = quantitative_phenotype_df.columns[1:]

# COMMAND ----------

hl_res_linreg = hl.read_table(quantitative_gwas_results_chk)
hl_res_linreg_df = explode_ht_to_df_linear(hl_res_linreg, quantitative_phenotypes).withColumn("hl_log_p", -fx.log10("hl_p_value")).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #### compare Hail to Glow linear regression

# COMMAND ----------

linear_results_df = spark.read.format("delta").load(linear_gwas_results_path). \
                                        withColumn("log_p", -fx.log10("pvalue"))

# COMMAND ----------

glow_hail_linreg_comparison = linear_results_df.join(hl_res_linreg_df, ["phenotype", "contigName", "start", "referenceAllele", "alternateAlleles"])
display(glow_hail_linreg_comparison)

# COMMAND ----------

glow_hail_linreg_comparison.stat.corr("log_p", "hl_log_p")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load logistic regression data

# COMMAND ----------

binary_phenotype_df = spark.createDataFrame(pd.read_csv(binary_phenotypes_path).set_index("sample_id", drop=False)). \
                     withColumn("sample_id", fx.col("sample_id").cast(StringType()))
binary_phenotypes = binary_phenotype_df.columns[1:]

# COMMAND ----------

hl_res_logreg = hl.read_table(binary_gwas_results_chk)
hl_res_logreg_df = explode_ht_to_df_logistic(hl_res_logreg, binary_phenotypes).withColumn("hl_log_p", -fx.log10("hl_p_value")).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #### compare Hail to Glow logistic regression

# COMMAND ----------

logistic_results_df = spark.read.format("delta").load(binary_gwas_results_path). \
                                 withColumn("log_p", -fx.log10("pvalue"))

# COMMAND ----------

glow_hail_logreg_comparison = logistic_results_df.join(hl_res_logreg_df, ["phenotype", "contigName", "start", "referenceAllele", "alternateAlleles"])
display(glow_hail_logreg_comparison)

# COMMAND ----------

glow_hail_logreg_comparison.stat.corr("log_p", "hl_log_p")