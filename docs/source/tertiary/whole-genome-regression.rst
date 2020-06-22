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

The first step in the fitting procedure is to apply a dimensionality reduction to the block matrix *X* using the
`RidgeReducer`. This is accomplished by fitting multiple ridge models within each block *x* and producing a new block
matrix where each column represents the prediction of one ridge model applied within one block. This approach to model
building is generally referred to as **stacking**. We will call the block genotype matrix we started with the
**level 0** matrix in the stack *X0*, and the output of the ridge reduction step the **level 1** matrix *X1*. The
`RidgeReducer` class is used for this step, which is initiallized with a list of ridge regularization values (referred
to here as alpha).  Since ridge models are indexed by these alpha values, the `RidgeReducer` will generate one ridge
model per value of alpha provided, which in turn will produce one column per block in *X0*, so the final dimensions of
matrix *X1* will be *Nx(LxK)*, where *L* is the number of header blocks in *X0* and *K* is the number of alpha values
provided to the `RidgeReducer`.  In practice, we can estimate a span of alpha values in a reasonable order of magnitude
based on guesses at the heritability of the phenotype we are fitting, but here we will just pick some values.

Initialization
==============

When the `RidgeReducer` is initialized, it will assign names to the provided alphas and store them in a dictionary
accessible as `RidgeReducer.alphas`.

Parameters
----------

Alphas...

Example
-------

.. code-block:: python

    alphas_reducer = np.logspace(2, 5, 10)
    reducer = RidgeReducer(alphas_reducer)

Model fitting
=============

The RidgeReducer.fit(blockdf, labeldf, indexdf) method generates a Spark DataFrame representing the model that we can
use to reduce *X0* to *X1*.

In explicit terms, the reduction of a block x0 from X0 to the corresponding block x1 from X1 is accomplished by the matrix multiplication x0 * B = x1, where B is a coefficient matrix of size mxK, where m is the number of columns in block x0 and K is the number of alpha values used in the reduction. As an added wrinkle, if the ridge reduction is being performed against multiple phenotypes at once, each phenotype will have its own B, and for convenience we panel these next to each other in the output into a single matrix, so B in that case has dimensions mx(KP)* where P is the number of phenotypes. Each matrix B is specific to a particular block in X0, so the Spark DataFrame produced by the RidgeReducer can be thought of all of as the matrices B from all of the blocks stacked one atop another. The fields in the model DataFrame are:

header_block: An ID assigned to the block x0 corresponding to the coefficients in t

Parameters
----------

- ``block_df``: Blocked genotype matrix.
- ``label_df``: Pandas DataFrame of phenotypic data, indexed by sample ID. Each column represents a single phenotype.
  We assume that there are no missing phenotype values, and that the phenotypes are mean centered at 0.
- ``sample_blocks``: Sample block mapping.
- ``covariates``: Optional Pandas DataFrame containing covariate data, indexed by sample ID.

Return
------

The ``fit`` functions returns a model DataFrame with the following fields:

- ``header_block``: An ID assigned to the block x0 corresponding to the coefficients in this row.
- ``sample_block``: An ID assigned to the block x0 corresponding to the coefficients in this row.
- ``header``: The name of a column from the conceptual matrix X0 that correspond with a particular row from the coefficient matrix B.
- ``alphas``: List of alpha names corresponding to the columns of B.
- ``labels``: List of label (i.e., phenotypes) corresponding to the columns of B.
- ``coefficients``: List of the actual values from a row in B

Model transformation
====================

Parameters
----------

Return
------

Example
=======

.. code-block:: python

    covariates = pd.read_csv(covariates_csv, index_col='sample_id')
    covariates['intercept'] = 1.

    label_df = pd.read_csv(continuous_phenotypes_csv, index_col='sample_id') \
        .apply(lambda x: x-x.mean())[['Continuous_Trait_1', 'Continuous_Trait_2']]
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

    alphas_regression = np.logspace(1, 4, 10)

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
    print(y_hat_df.at[('22', 'HG00096'),'Continuous_Trait_1'])
    assert math.isclose(y_hat_df.at[('22', 'HG00096'),'Continuous_Trait_1'], -0.48094813262232955)
