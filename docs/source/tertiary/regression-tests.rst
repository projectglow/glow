==============================================
Genome-wide Association Study Regression Tests
==============================================

.. invisible-code-block: python

    import glow
    glow.register(spark)

    path = 'test-data/1000G.phase3.broad.withGenotypes.chr20.10100000.vcf'

Glow contains functions for performing simple regression analyses used in
genome-wide association studies (GWAS).

.. _linear-regression:

Linear regression
=================

``linear_regression_gwas`` performs a linear regression association test optimized for performance
in a GWAS setting.

Example
-------

.. code-block:: python

  from pyspark.ml.linalg import DenseMatrix
  import pyspark.sql.functions as fx
  import numpy as np

  # Read in VCF file
  df = spark.read.format('vcf') \
    .option("splitToBiallelic", True) \
    .load(path) \
    .cache()

  # Generate random phenotypes and an intercept-only covariate matrix
  n_samples = df.select(fx.size('genotypes')).first()[0]
  covariates = DenseMatrix(n_samples, 1, np.ones(n_samples))
  np.random.seed(500)
  phenotypes = np.random.random(n_samples).tolist()
  covariates_and_phenotypes = spark.createDataFrame([[covariates, phenotypes]],
    ['covariates', 'phenotypes'])

  # Run linear regression test
  lin_reg_df = df.crossJoin(covariates_and_phenotypes).selectExpr(
    'contigName',
    'start',
    'names',
    # genotype_states returns the number of alt alleles for each sample
    'expand_struct(linear_regression_gwas(genotype_states(genotypes), phenotypes, covariates))')

.. invisible-code-block: python

   from pyspark.sql import Row
   assert_rows_equal(lin_reg_df.head(), Row(contigName='20', start=10000053, names=[], beta=-0.012268942487586866, standardError=0.03986890589124242, pValue=0.7583114855349732))


Parameters
----------

.. list-table::
  :header-rows: 1

  * - Name
    - Type
    - Details
  * - ``genotypes``
    - ``array<double>`` (or numeric type that can be cast to ``double``)
    - A numeric representation of the genotype for each sample at a given site, for example the
      result of the ``genotype_states`` function. This parameter can vary for each row in the dataset.
  * - ``covariates``
    - ``spark.ml`` ``Matrix``
    - A matrix containing the covariates to use in the linear regression model. Each row in the
      matrix represents observations for a sample. The indexing must match that of the ``genotypes``
      array that is, the 0th row in the covariate matrix should correspond to the same sample as the
      0th element in the ``genotypes`` array. This matrix must be constant for each row in the
      dataset. If desired, you must explicitly include an intercept covariate in this matrix.
  * - ``phenotypes``
    - ``array<double>`` (or numeric type that can be cast to ``double``)
    - A numeric representation of the phenotype for each sample. This parameter may vary for each
      row in the dataset. The indexing of this array must match the ``genotypes`` and
      ``covariates`` parameters.

Return
------

The function returns a struct with the following fields. The computation of each value matches the
`lm R package <https://stat.ethz.ch/R-manual/R-patched/library/stats/html/lm.html>`_.

.. list-table::
  :header-rows: 1

  * - Name
    - Type
    - Details
  * - ``beta``
    - ``double``
    - The fit effect coefficient of the ``genotypes`` parameter.
  * - ``standardError``
    - ``double``
    - The standard error of ``beta``.
  * - ``pValue``
    - ``double``
    - The P-value of the t-statistic for ``beta``.

Implementation details
----------------------

The linear regression model is fit using the QR decomposition. For performance, the QR decomposition
of the covariate matrix is computed once and reused for each (``genotypes``, ``phenotypes``) pair.

.. _logistic-regression:

Logistic regression
===================

``logistic_regression_gwas`` performs a logistic regression hypothesis test optimized for performance
in a GWAS setting.

Example
-------

.. code-block:: python

  # Likelihood ratio test
  log_reg_df = df.crossJoin(covariates_and_phenotypes).selectExpr(
    'contigName',
    'start',
    'names',
    'expand_struct(logistic_regression_gwas(genotype_states(genotypes), phenotypes, covariates, \'LRT\'))')

  # Firth test
  firth_log_reg_df = df.crossJoin(covariates_and_phenotypes).selectExpr(
    'contigName',
    'start',
    'names',
    'expand_struct(logistic_regression_gwas(genotype_states(genotypes), phenotypes, covariates, \'Firth\'))')

.. invisible-code-block: python

   assert_rows_equal(log_reg_df.head(), Row(contigName='20', start=10000053, names=[], beta=-0.04909334516505058, oddsRatio=0.9520922523419953, waldConfidenceInterval=[0.5523036168612923, 1.6412705426792646], pValue=0.8161087491239676))
   assert_rows_equal(firth_log_reg_df.head(), Row(contigName='20', start=10000053, names=[], beta=-0.04737592899383216, oddsRatio=0.9537287958835796, waldConfidenceInterval=[0.5532645977026418, 1.644057147112848], pValue=0.8205226692490032))


Parameters
----------

The parameters for the logistic regression test are largely the same as those for linear regression. The primary
differences are that the ``phenotypes`` values should be in the set ``[0,1]`` and that there is one additional
parameter ``test`` to specify the hypothesis test method.

.. list-table::
  :header-rows: 1

  * - Name
    - Type
    - Details
  * - ``genotypes``
    - ``array<double>`` (or numeric type that can be cast to ``double``)
    - A numeric representation of the genotype for each sample at a given site, for example the
      result of the ``genotype_states`` function. This parameter can vary for each row in the dataset.
  * - ``covariates``
    - ``spark.ml`` ``Matrix``
    - A matrix containing the covariates to use in the logistic regression model. Each row in the
      matrix represents observations for a sample. The indexing must match that of the ``genotypes``
      array that is, the 0th row in the covariate matrix should correspond to the same sample as the
      0th element in the ``genotypes`` array. This matrix must be constant for each row in the
      dataset. If desired, you must explicitly include an intercept covariate in this matrix.
  * - ``phenotypes``
    - ``array<double>`` (or numeric type that can be cast to ``double``)
    - A numeric representation of the phenotype for each sample. This parameter may vary for each
      row in the dataset. The indexing of this array must match the ``genotypes`` and
      ``covariates`` parameters.
  * - ``test``
    - ``string``
    - The hypothesis test method to use. Currently likelihood ratio (``LRT``) and Firth 
      (``Firth``) tests are supported.

Return
------

The function returns a struct with the following fields. The computation of each value matches the
`glm R package <https://stat.ethz.ch/R-manual/R-patched/library/stats/html/glm.html>`_ for the
likelihood ratio test and the
`logistf R package <https://cran.r-project.org/web/packages/logistf/logistf.pdf>`_ for the Firth
test.

.. list-table::
  :header-rows: 1

  * - Name
    - Type
    - Details
  * - ``beta``
    - ``double``
    - Log-odds associated with the ``genotypes`` parameter, ``NaN`` if the fit failed.
  * - ``oddsRatio``
    - ``double``
    - Odds ratio associated with the ``genotypes`` parameter, ``NaN`` if the fit failed..
  * - ``waldConfidenceInterval``
    - ``array<double>``
    - Wald 95% confidence interval of the odds ratio, ``NaN`` s if the fit failed.
  * - ``pValue``
    - ``double``
    - p-value for the specified ``test``. For the Firth test, this value is computed using the
      profile likelihood method. ``NaN`` if the fit failed.

Implementation details
----------------------

The logistic regression null model and fully-specified model are fit using Newton iterations. For performance, the null
model is computed once for each ``phenotype`` and used as a prior for each (``genotypes``, ``phenotypes``) pair.

Example notebook and blog post
------------------------------

A detailed example and explanation of a GWAS workflow is available `here <https://databricks.com/blog/2019/09/20/engineering-population-scale-genome-wide-association-studies-with-apache-spark-delta-lake-and-mlflow.html>`_.

.. notebook:: .. tertiary/gwas.html
  :title: GWAS notebook
