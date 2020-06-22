=======================
Whole-Genome Regression
=======================

.. invisible-code-block: python

    import glow
    glow.register(spark)

    genotypes_vcf = 'test-data/gwas/genotypes.vcf.gz'
    covariates_csv = 'test-data/gwas/covariates.csv.gz'
    continuous_phenotypes_csv = 'test-data/gwas/continuous-phenotypes.csv.gz'

Glow supports Whole Genome Regression (WGR) as GlowGR, a parallelized version of the
`regenie <https://www.biorxiv.org/content/10.1101/2020.06.19.162354v1>` method.

GlowGR consists of the following stages:
- Blocking the genotype matrix across samples and variants.
- Performing dimension reduction with ridge regression.
- Estimating phenotypic values with ridge regression.

------------------------
Blocking genotype matrix
------------------------

``glow.wgr.functions.block_variants_and_samples`` creates two objects: a block genotype matrix and a sample block
mapping.

Parameters
==========

- ``genotypes``: Genotype DataFrame created by reading from any variant datasource supported by Glow, such as VCF. Must
  also include a column ``values`` containing a numeric representation of each genotype, which cannot be the same
  across all samples in a variant.
- ``sample_ids``: List of sample IDs. Can be created by applying ``glow.wgr.functions.get_sample_ids`` to a genotype
  DataFrame.
- ``variants_per_block``: Number of variants to include per block.
- ``sample_block_count``: Number of sample blocks to create.

Return
======

The function returns a block genotype matrix and a sample block mapping.

Block genotype matrix
---------------------

If we imagine the block genotype matrix conceptually, we think of an *NxM* matrix *X* where each row *n* represents an
individual sample, each column *m* represents a variant, and each cell *(n, m)* contains a genotype value for sample *n*
at variant *m*.  We then imagine laying a coarse grid on top of this matrix such that matrix cells within the same
coarse grid cell are all assigned to the same block *x*.  Each block *x* is indexed by a sample block ID (corresponding
to a list of rows belonging to the block) and a header block ID (corresponding to a list of columns belonging to the
block).  The sample block IDs are generally just integers 0 through the number of sample blocks.  The header block IDs
are strings of the form 'chr_C_block_B', which refers to the Bth block on chromosome C.  The Spark DataFrame
representing this block matrix can be thought of as the transpose of each block *xT* all stacked one atop another.  Each
row represents the values from a particular column from *X*, for the samples corresponding to a particular sample block.
The fields in the DataFrame are:

- ``header``: A column name in the conceptual matrix *X*.
- ``size``: The number of individuals in the sample block for the row.
- ``values``: Genotype values for this header in this sample block.  If the matrix is sparse, contains only non-zero values.
- ``header_block``: An ID assigned to the block *x* containing this header.
- ``sample_block``: An ID assigned to the block *x* containing the group of samples represented on this row.
- ``position``:  An integer assigned to this header that specifies the correct sort order for the headers in this block.
- ``mu``: The mean of the genotype calls for this header.
- ``sig``: The standard deviation of the genotype calls for this header.

Sample block mapping
--------------------

The sample block mapping consists of key-value pairs, where each key is a sample block ID and each value is a list of
sample IDs contained in that sample block. The order of these IDs match the order of the ``values`` arrays in the block
genotype DataFrame.

Example
=======

.. code-block:: python

    from glow.wgr.linear_model import RidgeReducer, RidgeRegression
    from glow.wgr.functions import block_variants_and_samples, get_sample_ids
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

------------------------
Dimensionality reduction
------------------------

``RidgeReducer`` performs dimensionality reduction on the blocked genotype matrix.

- ``fit``
- ``transform``
- ``fit_transform``

Example
=======

.. code-block:: python

    covariates = pd.read_csv(covariates_csv, index_col='sample_id')
    covariates['intercept'] = 1.

    label_df = pd.read_csv(continuous_phenotypes_csv, index_col='sample_id') \
        .apply(lambda x: x-x.mean())[['Continuous_Trait_1', 'Continuous_Trait_2']]
    alphas_reducer = np.logspace(2, 5, 10)
    alphas_regression = np.logspace(1, 4, 10)

    reducer = RidgeReducer(alphas_reducer)
    reduced_block_df = reducer.fit_transform(block_df, label_df, sample_blocks, covariates)

--------------------------
Estimate phenotypic values
--------------------------

``RidgeRegression `` finds and applies an optimal model to calculate estimated phenotypic values.
- ``fit``
- ``transform``
- ``fit_transform`` uses the same blocked genotype matrix, phenotype DataFrame, sample block mapping, and covariates

Example
=======

.. code-block:: python

    regression = RidgeRegression(alphas_regression)
    model_df, cv_df = regression.fit(reduced_block_df, label_df, sample_blocks, covariates)
    all_contigs = [r.header_block for r in reduced_block_df.select('header_block').distinct().collect()]
    all_y_hat_df = pd.DataFrame()

    for contig in all_contigs:
      loco_reduced_block_df = reduced_block_df.filter(col('header_block') != lit(contig))
      loco_model_df = model_df.filter(~col('header').startswith(contig))
      loco_y_hat_df = regression.transform(loco_reduced_block_df, label_df, sample_blocks, loco_model_df, cv_df, covariates)
      loco_y_hat_df['contigName'] = contig.split('_')[1]
      all_y_hat_df = all_y_hat_df.append(loco_y_hat_df)
    y_hat_df = all_y_hat_df.reset_index().set_index(['contigName', 'sample_id'])

.. invisible-code-block: python

    import math

    print(y_hat_df)
    assert math.isclose(y_hat.at[('22', 'HG00096'),'Continuous_Trait_1'], -0.37493755917205657)
