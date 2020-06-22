=======================
Whole-Genome Regression
=======================

.. invisible-code-block: python

    import glow
    glow.register(spark)

    genotypes_vcf = 'test-data/gwas/genotypes.vcf.gz'
    covariates_csv = 'test-data/gwas/covariates.csv.gz'
    continuous_phenotypes_csv = 'test-data/gwas/continuous-phenotypes.csv.gz'

Glow supports Whole Genome Regression (WGR) as GlowGR, a parallelized version of the regenie method.

GlowGR consists of the following stages:
- Blocking the genotype matrix across samples and variants.
- Performing dimension reduction with ridge regression.
- Estimating phenotypic values with ridge regression.

.. code-block:: python

    from glow.levels.linear_model import RidgeReducer, RidgeRegression
    from glow.levels.functions import block_variants_and_samples, get_sample_ids
    import numpy as np
    import pandas as pd
    from pyspark.sql.functions import col, lit

    variants_per_block = 5
    sample_block_count = 10
    variants = spark.read.format('vcf').load(genotypes_vcf)
    genotypes = glow.transform('split_multiallelics', variants) \
        .withColumn('values', glow.mean_substitute(glow.genotype_states(col('genotypes')))) \
        .filter('size(array_distinct(values)) > 1') \
        .cache()
    sample_ids = get_sample_ids(genotypes)
    block_df, sample_blocks = block_variants_and_samples(
        genotypes, sample_ids, variants_per_block, sample_block_count)
    covariates = pd.read_csv(covariates_csv, index_col='sample_id')
    covariates['intercept'] = 1.

Linear model
============

Estimate phenotypic values
--------------------------

If the alpha hyperparameter values for ridge reduction and regression are not provided, they will be generated based on
the unique number of headers in the blocked genotype matrix `v`, and a set of heritability values.

.. math::

    \vec{\alpha} = v / 0.01, 0.25, 0.50, 0.75, 0.99]

.. warning::

    The phenotypes must be mean-centered at 0. The generated alpha values are only sensible if the phenotypes are also
    on the scale of one.

.. code-block:: python

    label_df = pd.read_csv(continuous_phenotypes_csv, index_col='sample_id') \
        .apply(lambda x: x-x.mean())[['Continuous_Trait_1', 'Continuous_Trait_2']]
    alphas_reducer = np.logspace(2, 5, 10)
    alphas_regression = np.logspace(1, 4, 10)

    reducer = RidgeReducer(alphas_reducer)
    reduced_block_df = reducer.fit_transform(block_df, label_df, sample_blocks, covariates)

    regression = RidgeRegression(alphas_regression)
    model_df, cv_df = regression.fit(reduced_block_df, label_df, sample_blocks, covariates)
    all_contigs = [r.header_block for r in reduced_block_df.select('header_block').distinct().collect()]
    all_y_hat_df = pd.DataFrame()

    reduced_block_df.show()
    model_df.show()
    model_df.filter(~col('header').startswith('chr_22')).show()

    for contig in all_contigs:
      loco_reduced_block_df = reduced_block_df.filter(col('header_block') != lit(contig))
      loco_model_df = model_df #.filter(~col('header_block').startswith(contig))
      loco_y_hat_df = regression.transform(loco_reduced_block_df, label_df, sample_blocks, loco_model_df, cv_df, covariates)
      loco_y_hat_df['contigName'] = contig.split('_')[1]
      all_y_hat_df = all_y_hat_df.append(loco_y_hat_df)
    y_hat_df = all_y_hat_df.reset_index().set_index(['contigName', 'sample_id'])

.. invisible-code-block: python

    import math

    print(y_hat_df)
    assert math.isclose(y_hat.at[('22', 'HG00096'),'Continuous_Trait_1'], -0.37493755917205657)
