=======================
Whole-Genome Regression
=======================

.. invisible-code-block: python

    import glow
    glow.register(spark)

    genotypes_vcf = 'test-data/gwas/genotypes.vcf.gz'
    covariates_csv = 'test-data/gwas/covariates.csv.gz'
    continuous_phenotypes_csv = 'test-data/gwas/continuous-phenotypes.csv.gz'

Glow contains functions for performing Whole Genome Regression (WGR).
WGR has two primary stages: reduction and regression. Both stages are regularized using ridge regression.
WGR operates on block genotype matrices. These can be created with our variant and sample blocking helper functions.

.. code-block:: python

    variants_per_block = 1000
    sample_block_count = 10
    variants = spark.read.format('vcf').load(genotypes_vcf)
    genotypes = glow.transform('split_multiallelics', variants) \
        .withColumn('values', glow.mean_substitute(glow.genotype_states(col('genotypes')))) \
        .filter('size(array_distinct(values)) > 1') \
        .cache()
    sample_ids = get_sample_ids(genotypes)
    block_df_lvl0, sample_blocks = block_variants_and_samples(
        genotypes, sample_ids, variants_per_block, sample_block_count)

    covariates = pd.read_csv(covariates_csv, index_col='sample_id')
    covariates['intercept'] = 1.

Linear model
============

The phenotypes must be mean-centered at 0.

.. code-block:: python

    label_df = pd.read_csv(continuous_phenotypes_csv, index_col='sample_id') \
        .apply(lambda x: x-x.mean())[['Trait_1', 'Trait_2']]
    alphas_lvl0 = np.logspace(2, 5, 10)
    alphas_lvl1 = np.logspace(1, 4, 10)
    alphas_lvl2 = np.logspace(0, 3, 10)

Reduction and regression
------------------------

Calculate the WGR-estimated phenotypes with a single round of reduction before the regression stage.

.. code-block:: python

    stack_lvl0 = RidgeReducer(alphas_lvl0)
    model_df_lvl0 = stack_lvl0.fit(block_df_lvl0, label_df, sample_blocks, covariates)
    block_df_lvl1 = stack_lvl0.transform(block_df_lvl0, label_df, sample_blocks, model_df_lvl0, covariates)

    estimator_lvl1 = RidgeRegression(alphas_lvl1)
    model_df_lvl1_est, cv_df_lvl1 = estimator_lvl1.fit(block_df_lvl1, label_df, sample_blocks, covariates)
    y_hat_one_round = estimator_lvl1.transform(block_df_lvl1, label_df, sample_blocks, model_df_lvl1_est, cv_df_lvl1, covariates)

Two rounds of reduction and regression
--------------------------------------

Calculate the WGR-estimated phenotypes with two rounds of reduction before the regression stage.

.. code-block:: python

    stack_lvl1 = RidgeReducer(alphas_lvl1)
    model_df_lvl1 = stack_lvl1.fit(block_df_lvl1, label_df, sample_blocks, covariates)
    block_df_lvl2 = stack_lvl1.transform(block_df_lvl1, label_df, sample_blocks, model_df_lvl1, covariates)

    estimator_lvl2 = RidgeRegression(alphas_lvl2)
    model_df_lvl2_est, cv_df_lvl2 = estimator_lvl2.fit(block_df_lvl2, label_df, sample_blocks, covariates)
    y_hat_two_rounds = estimator_lvl2.transform(block_df_lvl2, label_df, sample_blocks, model_df_lvl2_est, cv_df_lvl2, covariates)

Two rounds of reduction and leave-one-chromosome-out regression
---------------------------------------------------------------

The Pandas DataFrame output by leave-one-chromosome-out (LOCO) regression is shaped differently. As the phenotype is
estimated on a per-chromosome basis, the DataFrame contains an additional column representing the chromosome. Also, the
number of rows is multiplied by the number of chromosomes.

.. code-block:: python

    all_contigs = [r.header_block for r in block_df_lvl1.select('header_block').distinct().collect()]
    y_hat_two_rounds_loco = pd.DataFrame()
    for contig in all_contigs:
      loco_block = block_df_lvl2.filter(f'header NOT LIKE "%block_{contig}%"')
      loco_df = estimator_lvl2.transform(loco_block, label_df, sample_blocks, model_df_lvl2_est, cv_df_lvl2, covariates)
      loco_df['contigName'] = contig.split('_')[1]
      y_hat_two_rounds_loco = y_hat_two_rounds_loco.append(loco_df)

GWAS
----

Use the estimated phenotypic values from WGR to adjust the phenotypes before running GWAS.

To perform GWAS with WGR-estimated phenotypes calculated by standard regression, subtract the estimated phenotypes from
the input phenotypes. The adjusted phenotypes hold across all sites, so perform a cross-join with the genotypes.

.. code-block:: python

    pdf = (label_df - y_hat_two_rounds).T
    apdf = pd.DataFrame()
    apdf['pt'] = pdf.values.tolist()
    apdf['trait'] = pdf.index
    adjusted_two_rounds = spark.createDataFrame(apdf)
    genotypes.crossJoin(adjusted_two_rounds).select(
        'contigName',
        'start',
        'names',
        'trait',
        expand_struct(linear_regression_gwas(
            col('values'),
            col('pt'),
            lit(covariates.to_numpy())
      )))


To perform GWAS with WGR-estimated phenotypes calculated by LOCO regression, subtract the estimated phenotypes from
the input phenotypes across all chromosomes. The adjusted phenotypes hold on a per-chromosome basis, so perform an
inner join with the genotypes based on chromosome name.

.. code-block:: python

    pdf = (label_df - y_hat_two_rounds_loco.reset_index().set_index(['contigName', 'sample_id']))
    apdf = pdf.reset_index('contigName') \
        .melt(id_vars=['contigName']) \
        .groupby(['contigName', 'variable']) \
        .aggregate(lambda x: list(x)) \
        .reset_index() \
        .rename(columns={'variable': 'trait', 'value': 'pt'})
    adjusted_two_rounds_loco = spark.createDataFrame(apdf)
    genotypes.join(adjusted_two_rounds_loco, ['contigName']).select(
        'contigName',
        'start',
        'names',
        'trait',
        expand_struct(linear_regression_gwas(
            col('values'),
            col('pt'),
            lit(covariates.to_numpy())
        )))

