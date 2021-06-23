# Databricks notebook source
# MAGIC %md
# MAGIC #### Setup deep null 
# MAGIC 
# MAGIC Deep null implements nonlinear covariate modeling to increase power in genome-wide association studies.
# MAGIC 
# MAGIC As described in, "DeepNull: Modeling non-linear covariate effects improves phenotype prediction and association power" ([Hormozdiari et al 2021](https://doi.org/10.1101/2021.05.26.445783))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. import notebook to your workspace
# MAGIC 
# MAGIC use this `url`:
# MAGIC 
# MAGIC https://github.com/Google-Health/genomics-research/blob/main/nonlinear-covariate-gwas/DeepNull_e2e.ipynb

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ###<img src="https://docs.databricks.com/_images/import-notebook.png" alt="logo" width="350"/> 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. setup cluster
# MAGIC 
# MAGIC Use a driver only cluster with the machine learning runtime

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. simulate phenotypes and covariates

# COMMAND ----------

import pandas as pd
import numpy as np
import random
import string

# COMMAND ----------

n_samples = 1000
n_phenotypes = 1
n_covariates = 3

# COMMAND ----------

user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
output_path = '/dbfs/home/{}/genomics/data/simulate_'.format(user) + str(n_samples) + '_samples_'
output_covariates_phenotypes = output_path + str(n_covariates) + '_covariates_' + str(n_phenotypes) + '_quantitative_phenotypes.csv'

# COMMAND ----------

# define random family IDs (FID)
FID = pd.DataFrame(['FID_' + ''.join(random.choice(string.ascii_uppercase) for i in range(7)) for i in range(n_samples)], columns=['FID'])
# define random individual IDs (IID)
IID = pd.DataFrame(['IID_' + ''.join(random.choice(string.ascii_uppercase) for i in range(7)) for i in range(n_samples)], columns=['IID'])
# define random covariates
covariates_quantitative =  pd.DataFrame(np.random.random((n_samples, n_covariates - 2)), 
                                           columns=['Q'+ str(i) for i in range(n_covariates - 2)])
covariates_binary = pd.DataFrame(np.random.randint(0, 2, (n_samples, 2)), 
                                           columns=['B'+ str(i) for i in range(2)])
# define random phenotypes
phenotypes_quantitative = pd.DataFrame(np.random.random((n_samples, n_covariates - 2)), 
                                           columns=['P'+ str(i) for i in range(n_covariates - 2)])

# COMMAND ----------

#merge into one pandas dataframe
covariates_phenotypes = pd.concat([FID, IID, covariates_binary, covariates_quantitative, phenotypes_quantitative], axis=1)
covariates_phenotypes.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. write to `/dbfs/`

# COMMAND ----------

covariates_phenotypes.to_csv(output_covariates_phenotypes, index=False, header=True, sep = '\t')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. run deep null notebook to model non-linear interactions between covariates
# MAGIC 
# MAGIC make the following changes below to the deepnull notebook:

# COMMAND ----------

# MAGIC %md
# MAGIC use the `%pip` magic command to install deepnull in a notebook

# COMMAND ----------

# MAGIC %pip install --upgrade deepnull

# COMMAND ----------

# MAGIC %md
# MAGIC Replace the `required inputs` cell in the deepnull notebook with the following:

# COMMAND ----------

n_samples = 1000
n_phenotypes = 1
n_covariates = 3

user=dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
output_path = '/dbfs/home/{}/genomics/data/simulate_'.format(user) + str(n_samples) + '_samples_'
input_tsv = output_path + str(n_covariates) + '_covariates_' + str(n_phenotypes) + '_quantitative_phenotypes.csv'

target_phenotype = 'P0'
covariate_predictors = 'B0, B1, Q0' 
num_folds = 5
input_data_missing_value = 'NA'
output_column_name = 'deepnull_prediction'
output_tsv = output_path + str(n_covariates) + '_covariates_' + str(n_phenotypes) + '_deepnull.csv'
random_seed = 14475

# COMMAND ----------

# MAGIC %md
# MAGIC ##### copy these cells over to the deepnull notebook and run!