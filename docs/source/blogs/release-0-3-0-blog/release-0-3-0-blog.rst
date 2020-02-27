======================
Glow 0.3.0 is Released
======================

| February 21, 2020

Glow 0.3.0 was recently released. This release contains some exciting new and improved features aimed at increasing the capabilities and comfort of using Glow to perform large-scale genomics analysis. In this blog, we present an overview of these new futures.


Python and Scala APIs for SQL functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In this release, we introduced Python and Scala APIs for all Glow SQL functions, which could only be used in SQL expressions in the previous version. These APIs are similar to those for Spark SQL functions and provide improved compile-time safety. The SQL functions and their Python and Scala clients are generated from the same source so any new functionality in the future will always appear in all three languages. See :ref:`pyspark_functions` for more information on Python APIs for Glow functions. Examples of Python and Scala API for the ``normalized_variants`` function can be seen in the next section.


An improved transformer to normalize variants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The variant normalizer went through a major improvement in this release. The normalizer still behaves like `bcftools norm <https://www.htslib.org/doc/bcftools.html#norm>`_ and `vt normalize <https://genome.sph.umich.edu/wiki/Vt#Normalization>`_, however it is now about 2.5x faster and has a much more flexible API. The new normalizer implemented both as a transformer and as a function:

**``normalize_variants`` transformer**: The new transformer preserves the columns of the input DataFrame and adds the normalization result (including the normalized coordinates, alleles, and status) as a new column to the DataFrame. The normalization status shows whether the normalization changed the variant and the error message in case of an error. The new ``replace_columns`` option can be used to replace the original ``start``, ``end``, ``referenceAllele``, and ``alternateAlleles`` fields with the normalization results. See :ref:`variantnormalization` for more details. Since the multiallelic variant splitter is implemented as a separate transformer in this release (see below), the ``mode`` option of the ``normalize_variants`` transformer is deprecated in this release.

**``normalize_variant`` function**: In this release, variant normalization can also be done using ``normalize_variant`` function. An example of the DataFrame original_variants_df loaded by the  following command in Python is shown in :numref:`figorigdf`.

.. code-block::

  original_variants_df = spark.read\
    .format("vcf")\
    .load("/databricks-datasets/genomics/call-sets")


.. figure:: figoriginaldf.png
   :align: center
   :width: 800
   :name: figorigdf


.. code-block::

  import glow
  from pyspark.sql.functions import expr

  ref_genome_path = '/mnt/dbnucleus/dbgenomics/grch38/data/GRCh38.fa'

  normalization_expr = "normalize_variant(contigName, start, end, referenceAllele, alternateAlleles, '{ref_genome}')".format(ref_genome=ref_genome_path)
  df_normalized = df_original.withColumn('normalizationResult', expr(normalization_expr))

Our example Dataframe after normalization can be seen in :numref:`fignormalizeddf`.

.. figure:: fignormalizeddf.png
   :width: 800
   :name: fignormalizeddf

A new transformer to split multi-allelic variants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This release includes the new DataFrame transformer ``split_multiallelics`` to split multi-allelic variants with a behavior similar to `vt decompose <https://genome.sph.umich.edu/wiki/Vt#Decompose>`_ with option ``-s`` in the vt package. This behavior is significantly more powerful compared to the behavior of ths splitter in the previous version which simply behaved like GATK's LeftAlignAndTrim with ``--splitmultiallelics`` option. In particular, the array-type INFO fields and genotype fields which have entries coresponding to each of alternate alleles and/or reference allelles as well as those that have entries corressponding to possible genotype calls sorted in colex order, such as ``GL``, ``PL``, and ``GP``, in VCF format are smartly split in this new transformer. Please refer to the documentation of the new splitter transformer for precise bahavior of the splitter.

Moreover, the new splitter is implemented as a separate transformer from the ``normlize_variants`` trnsformer, as opposed to the previous versions where the splitting could only be done as one of the modes of the ``normalize_variant`` transformer (this usage is deprecated in this release).




- **Parser of ``ANN/CSQ`` fields from VCF** The vcf reader and the pipe transformer in the new release are now able to parse ``ANN`` and ``CSQ`` fields into a structured format instead of putting them as strings in the ``INFO`` field. This makes queries simpler to write and much faster.
- **Improved variant normalizer**.  is now implemented as a SQL function as well as a transformer, and follows the same logic as ``bcftools norm`` and ``vt normalize``. The normalizer is also about 2.5x faster than the previous version. See [here](https://glow.readthedocs.io/en/latest/etl/variant-normalization.html) for documentation.
- #95, #140: Significant performance improvements, ~50%, were made to the linear and logistic regression functions.
- #137: Support for Scala 2.12 was added.
