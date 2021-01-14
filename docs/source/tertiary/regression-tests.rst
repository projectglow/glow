.. _gwas:

=============================================================
GloWGR: Genome-Wide Association Study (GWAS) Regression Tests
=============================================================

.. invisible-code-block: python

    import glow
    glow.register(spark)

    genotypes_vcf = 'test-data/gwas/genotypes.vcf.gz'
    covariates_csv = 'test-data/gwas/covariates.csv.gz'
    continuous_phenotypes_csv = 'test-data/gwas/continuous-phenotypes.csv.gz'
    continuous_offset_csv = 'test-data/gwas/continuous-offsets.csv.gz'
    binary_phenotypes_csv = 'test-data/gwas/binary-phenotypes.csv.gz'
    binary_offset_csv = 'test-data/gwas/binary-offsets.csv.gz'

Glow contains functions for performing regression analyses used in
genome-wide association studies (GWAS). These functions are best used in conjunction with the
:ref:`GloWGR whole genome regression method <glowgr>`, but also work as standalone
analysis tools.

.. tip::
  Glow automatically converts literal one-dimensional and two-dimensional ``numpy`` ``ndarray`` s of ``double`` s
  to ``array<double>`` and ``spark.ml`` ``DenseMatrix`` respectively.

.. _linear-regression:

Linear regression
=================

``linear_regression`` performs a linear regression association test optimized for performance
in a GWAS setting. You provide a Spark DataFrame containing the genetic data and Pandas DataFrames
with the phenotypes, covariates, and optional offsets (typically predicted phenotypes from
GloWGR). The function returns a Spark DataFrame with association test results for each
(variant, phenotype) pair.

Each worker node in the cluster tests a subset of the total variant dataset. Multiple phenotypes
and variants are tested together to take advantage of efficient matrix-matrix linear algebra
primitives.

Example
-------

.. code-block:: python

  import glow
  import numpy as np
  import pandas as pd
  from pyspark.sql import Row
  from pyspark.sql.functions import col, lit

  # Read in VCF file
  variants = spark.read.format('vcf').load(genotypes_vcf)

  # genotype_states returns the number of alt alleles for each sample
  # mean_substitute replaces any missing genotype states with the mean of the non-missing states
  genotypes = (glow.transform('split_multiallelics', variants)
    .withColumn('gt', glow.mean_substitute(glow.genotype_states(col('genotypes'))))
    .select('contigName', 'start', 'names', 'gt')
    .cache())

  # Read covariates from a CSV file
  covariates = pd.read_csv(covariates_csv, index_col=0)

  # Read phenotypes from a CSV file
  phenotypes = pd.read_csv(continuous_phenotypes_csv, index_col=0)

  # Run linear regression test
  lin_reg_df = glow.gwas.linear_regression(genotypes, phenotypes, covariates, values_column='gt')

.. invisible-code-block: python

   expected_lin_reg_row = Row(
     contigName='22',
     start=16050114,
     names=['rs587755077'],
     phenotype='Continuous_Trait_1',
     effect=0.14722512852575978,
     stderror=0.14155327969643167,
     pvalue=0.2984087428847886,
     tvalue=1.0400686500623064
   )
   assert_rows_equal(lin_reg_df.filter('contigName = 22 and start = 16050114').head(), expected_lin_reg_row)

The linear regression function accepts GloWGR phenotypic predictions (either global or per chromosome) as an offset.

.. code-block:: python

  offsets = pd.read_csv(continuous_offset_csv, index_col=0)
  lin_reg_df = glow.gwas.linear_regression(genotypes, phenotypes, covariates, offset_df=offsets, values_column='gt')

.. invisible-code-block: python

   expected_lin_reg_row = Row(
     contigName='22',
     start=16050114,
     names=['rs587755077'],
     effect=0.14153340605722264,
     stderror=0.17619727316255493,
     tvalue=0.8032667221055554,
     pvalue=0.42189707280260846,
     phenotype='Continuous_Trait_1')
   assert_rows_equal(lin_reg_df.filter('contigName = 22 and start = 16050114').head(), expected_lin_reg_row)

For complete parameter usage information, check out the API reference for :func:`glow.gwas.linear_regression`.

.. note::

  Glow also includes a SQL-based function for performing linear regression. However, this function
  only processes one phenotype at time, and so performs more slowly than the batch linear regression function
  documented above. To read more about the SQL-based function, see the docs for
  :func:`linear_regression <glow.functions.linear_regression_gwas>`.

.. _logistic-regression:

Logistic regression
===================

``logistic_regression`` performs a logistic regression hypothesis test optimized for performance
in a GWAS setting.

Example
-------

.. code-block:: python

  import glow
  import numpy as np
  import pandas as pd
  from pyspark.sql import Row
  from pyspark.sql.functions import col, lit

  # Read in VCF file
  variants = spark.read.format('vcf').load(genotypes_vcf)

  # genotype_states returns the number of alt alleles for each sample
  # mean_substitute replaces any missing genotype states with the mean of the non-missing states
  genotypes = (glow.transform('split_multiallelics', variants)
    .withColumn('gt', glow.mean_substitute(glow.genotype_states(col('genotypes'))))
    .select('contigName', 'start', 'names', 'gt')
    .cache())

  # Read covariates from a CSV file
  covariates = pd.read_csv(covariates_csv, index_col=0)

  # Read phenotypes from a CSV file
  phenotypes = pd.read_csv(binary_phenotypes_csv, index_col=0)

  # Run logistic regression test with approximate Firth correction for p-values below 0.05
  log_reg_df = glow.gwas.logistic_regression(
    genotypes,
    phenotypes,
    covariates,
    correction='approx-firth',
    pvalue_threshold=0.05,
    values_column='gt'
  )

The logistic regression function accepts GloWGR phenotypic predictions (either global or per chromosome) as an offset.

.. code-block:: python

  offsets = pd.read_csv(binary_offset_csv, index_col=0)
  log_reg_df = glow.gwas.logistic_regression(
    genotypes,
    phenotypes,
    covariates,
    offset_df=offsets,
    correction='approx-firth',
    pvalue_threshold=0.05,
    values_column='gt'
  )

.. tip::

 The ``offset`` parameter is especially useful in incorporating the results of :ref:`GloWGR <glowgr>` with
 binary phenotypes in GWAS. Please refer to :ref:`glowgr` for details and
 example notebook.

For complete parameter usage information, check out the API reference for :func:`glow.gwas.logistic_regression`.

.. note::

  Glow also includes a SQL-based function for performing logistic regression. However, this function
  only processes one phenotype at time, and so performs more slowly than the batch logistic regression function
  documented above. To read more about the SQL-based function, see the docs for
  :func:`logistic_regression <glow.functions.logistic_regression_gwas>`.

Example notebook and blog post
------------------------------

A detailed example and explanation of a GWAS workflow is available `here <https://databricks.com/blog/2019/09/20/engineering-population-scale-genome-wide-association-studies-with-apache-spark-delta-lake-and-mlflow.html>`_.

.. notebook:: .. tertiary/gwas.html
  :title: GWAS notebook
