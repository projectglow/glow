.. _gwas:

======================================================
GloWGR: Genome-wide Association Study Regression Tests
======================================================

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
in a GWAS setting. It uses vectorized linear algebra libraries to process multiple phenotypes
efficiently.

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
  documented above. To read more about the SQL-based function, see the docs for :func:`linear_regression <glow.functions.linear_regression_gwas>`.

.. _logistic-regression:

Logistic regression
===================

``logistic_regression_gwas`` performs a logistic regression hypothesis test optimized for performance
in a GWAS setting.

Example
-------

.. code-block:: python

  # Read a single phenotype from a CSV file
  trait = 'Binary_Trait_1'
  phenotype = np.hstack(pd.read_csv(binary_phenotypes_csv, index_col=0)[[trait]].to_numpy()).astype('double')

  # Likelihood ratio test
  lrt_log_reg_df = genotypes.select(
    'contigName',
    'start',
    'names',
    glow.expand_struct(glow.logistic_regression_gwas(
      col('gt'),
      lit(phenotype),
      lit(covariates.to_numpy()),
      'LRT'
    ))
  )

  # Firth test
  firth_log_reg_df = genotypes.select(
    'contigName',
    'start',
    'names',
    glow.expand_struct(glow.logistic_regression_gwas(
      col('gt'),
      lit(phenotype),
      lit(covariates.to_numpy()),
      'Firth'
    ))
  )

  # Logistic regression with offset

  # Read offset from a csv file
  offset = np.hstack(pd.read_csv(binary_offset_csv, index_col=0)[[trait]].to_numpy()).astype('double')

  # LRT test with offset
  lrt_log_reg_with_offset_df = genotypes.select(
    'contigName',
    'start',
    'names',
    glow.expand_struct(glow.logistic_regression_gwas(
      col('gt'),
      lit(phenotype),
      lit(covariates.to_numpy()),
      'LRT',
      lit(offset)
    ))
  )

  # Firth test with offset
  firth_log_reg_with_offset_df = genotypes.select(
    'contigName',
    'start',
    'names',
    glow.expand_struct(glow.logistic_regression_gwas(
      col('gt'),
      lit(phenotype),
      lit(covariates.to_numpy()),
      'Firth',
      lit(offset)
    ))
  )


.. invisible-code-block: python

   expected_lrt_log_reg_row = Row(
     contigName='22',
     start=16050114,
     names=['rs587755077'],
     beta=0.6505788739813515,
     oddsRatio=1.916650006629025,
     waldConfidenceInterval=[0.8928977733259339, 4.114185697011577],
     pValue=0.09477605005654555
   )
   assert_rows_equal(lrt_log_reg_df.filter('contigName = 22 and start = 16050114').head(), expected_lrt_log_reg_row)

   expected_firth_log_reg_row = Row(
     contigName='22',
     start=16050114,
     names=['rs587755077'],
     beta=0.6432946160462902,
     oddsRatio=1.9027393596251838,
     waldConfidenceInterval=[0.8867936962411556, 4.082592248921799],
     pValue=0.09324599164678671
   )
   assert_rows_equal(firth_log_reg_df.filter('contigName = 22 and start = 16050114').head(), expected_firth_log_reg_row)

   expected_lrt_log_reg_with_offset_row = Row(
     contigName='22',
     start=16050114,
     names=['rs587755077'],
     beta=0.6532532933961581,
     oddsRatio=1.9217827931701041,
     waldConfidenceInterval=[0.8951374441408548, 4.1259017017989175],
     pValue=0.09351514539362338
   )
   assert_rows_equal(lrt_log_reg_with_offset_df.filter('contigName = 22 and start = 16050114').head(), expected_lrt_log_reg_with_offset_row)

   expected_firth_log_reg_with_offset_row = Row(
     contigName='22',
     start=16050114,
     names=['rs587755077'],
     beta=0.645985598762526,
     oddsRatio=1.9078664937761651,
     waldConfidenceInterval=[0.8890313085248763, 4.094292881668416],
     pValue=0.0919716153186052
   )
   assert_rows_equal(firth_log_reg_with_offset_df.filter('contigName = 22 and start = 16050114').head(), expected_firth_log_reg_with_offset_row)

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
  * - ``offset`` (optional)
    - ``array<double>`` (or numeric type that can be cast to ``double``)
    - An offset vector to be added to the linear predictor terms. The offset term is added with the fixed
      coefficient 1 (its coefficient will not be estimated). This parameter may vary for each
      row in the dataset. The indexing of this array must match the ``phenotypes``.

.. tip::

 The ``offset`` parameter is especially useful in incorporating the results of :ref:`GloWGR <glowgr>` with
 binary phenotypes in GWAS. Please refer to :ref:`glowgr` for details and
 example notebook.

Return
------

The function returns a struct with the following fields. The computation of each value matches the
`glm R package <https://www.rdocumentation.org/packages/stats/versions/3.6.1/topics/glm>`_ for the
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
