# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Simulate random binary / quantitative covariates and phenotypes

# COMMAND ----------

# MAGIC %md ##### setup constants

# COMMAND ----------

# MAGIC %run ../0_setup_constants_glow

# COMMAND ----------

# MAGIC %md
# MAGIC ##### simulate covariates helper functions

# COMMAND ----------

def np_array_to_pandas_with_missing(np_array, missingness, n_cols, col_prefix='Q'):
  pdf =  pd.DataFrame(np_array, columns=[col_prefix + str(i+1) for i in range(n_cols)])
  pdf = pdf.mask(np.random.choice([True, False], size=pdf.shape, p=[missingness, 1- missingness]))
  return pdf
 
def add_sample_index_pdf(pdf, sample="sample_id"):
  pdf.index.name = "sample_id"
  pdf.index = pdf.index.map(str)
  return pdf

# COMMAND ----------

# MAGIC %md
# MAGIC ##### simulate covariates

# COMMAND ----------

covariates_quantitative = np.random.random((n_samples, n_quantitative_covariates))
covariates_quantitative_pdf = np_array_to_pandas_with_missing(covariates_quantitative, 0, n_quantitative_covariates, col_prefix='Q')

# COMMAND ----------

covariates_quantitative_pdf

# COMMAND ----------

covariates_binary = np.random.randint(0, 2, (n_samples, n_binary_covariates))
covariates_binary_pdf = np_array_to_pandas_with_missing(covariates_binary, 0, n_binary_covariates, col_prefix='B')
covariates_binary_pdf = covariates_binary_pdf.astype(pd.Int64Dtype())
covariates_binary_pdf

# COMMAND ----------

covariates = pd.concat([covariates_binary_pdf, covariates_quantitative_pdf], axis=1)
covariates = add_sample_index_pdf(covariates)
covariates.head(5)

# COMMAND ----------

covariates.to_csv(output_covariates, index=True, header=True, sep = ',')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### simulate phenotypes

# COMMAND ----------

binary_phenotypes = np.random.randint(0, 2, (n_samples, n_binary_phenotypes))
binary_phenotypes_pdf = np_array_to_pandas_with_missing(binary_phenotypes, missingness, n_binary_phenotypes, col_prefix='BP')
binary_phenotypes_pdf = add_sample_index_pdf(binary_phenotypes_pdf)
binary_phenotypes_pdf.head(10)

# COMMAND ----------

binary_phenotypes_pdf.to_csv(output_binary_phenotypes, index=True, header=True, sep = ',')

# COMMAND ----------

quantitative_phenotypes = np.random.normal(loc=0.0, scale=1.0, size=(n_samples, n_quantitative_phenotypes))
quantitative_phenotypes_pdf = np_array_to_pandas_with_missing(quantitative_phenotypes, missingness, n_quantitative_phenotypes, col_prefix='QP')
quantitative_phenotypes_pdf = add_sample_index_pdf(quantitative_phenotypes_pdf)
quantitative_phenotypes_pdf.head(5)

# COMMAND ----------

quantitative_phenotypes_pdf.to_csv(output_quantitative_phenotypes, index=True, header=True, sep = ',')