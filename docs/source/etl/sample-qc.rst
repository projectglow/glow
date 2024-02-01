.. _sample-qc:

======================
Sample Quality Control
======================

You can calculate quality control statistics on your variant data using Spark SQL functions, which
can be expressed in Python, R, Scala, or SQL.

Each of these functions returns an array of structs containing metrics for one sample. If sample ids
are including in the input DataFrame, they will be propagated to the output. The functions assume
that the genotypes in each row of the input DataFrame contain the same samples in the same order.

.. list-table::
  :header-rows: 1

  * - Functions
    - Arguments
    - Return
  * - ``sample_call_summary_stats``
    - ``referenceAllele`` string, ``alternateAlleles`` array of strings, ``genotypes`` array ``calls``
    - A struct containing the following summary stats:

      * ``callRate``: The fraction of variants where this sample has a called genotype. Equivalent to
        ``nCalled / (nCalled + nUncalled)``
      * ``nCalled``: The number of variants where this sample has a called genotype
      * ``nUncalled``: The number of variants where this sample does not have a called genotype
      * ``nHomRef``: The number of variants where this sample is homozygous reference
      * ``nHet``: The number of variants where this sample is heterozygous
      * ``nHomVar``: The number of variants where this sample is homozygous non reference
      * ``nSnv``: The number of calls where this sample has a single nucleotide variant. This value is the sum of ``nTransition`` and ``nTransversion``
      * ``nInsertion``: Insertion variant count
      * ``nDeletion``: Deletion variant count
      * ``nTransition``: Transition count
      * ``nTransversion``: Transversion count
      * ``nSpanningDeletion``: The number of calls where this sample has a spanning deletion
      * ``rTiTv``: Ratio of transitions to tranversions (``nTransition / nTransversion``)
      * ``rInsertionDeletion``: Ratio of insertions to deletions (``nInsertion / nDeletion``)
      * ``rHetHomVar``: Ratio of heterozygous to homozygous variant calls (``nHet / nHomVar``)
  * - ``sample_dp_summary_stats``
    - ``genotypes`` array with a ``depth`` field
    - A struct with ``min``, ``max``, ``mean``, and ``stddev``
  * - ``sample_gq_summary_stats``
    - ``genotypes`` array with a ``conditionalQuality`` field
    - A struct with ``min``, ``max``, ``mean``, and ``stddev``

Computing user-defined sample QC metrics
----------------------------------------

In addition to the built-in QC functions discussed above, Glow provides two ways to compute
user-defined per-sample statistics.

Explode and aggregate
~~~~~~~~~~~~~~~~~~~~~

If your dataset is not in a normalized, pVCF-esque shape, or if you want the aggregation output in a
table rather than a single array, you can explode the ``genotypes`` array and use any of the
aggregation functions built into Spark. For example, this code snippet computes the number of sites
with a non-reference allele for each sample:

.. invisible-code-block: python

  df = spark.read.format('vcf').load('test-data/combined.chr20_18210071_18210093.g.vcf')

.. code-block:: python
  
  import pyspark.sql.functions as fx
  exploded_df = df.withColumn("genotype", fx.explode("genotypes"))\
    .withColumn("hasNonRef", fx.expr("exists(genotype.calls, call -> call != -1 and call != 0)"))

  agg = exploded_df.groupBy("genotype.sampleId", "hasNonRef")\
    .agg(fx.count(fx.lit(1)))\
    .orderBy("sampleId", "hasNonRef")
  
.. invisible-code-block: python

  from pyspark.sql import Row
  expected_agg = Row(sampleId='HG00096', count=22, hasNonRef=False)
  assert_rows_equal(agg.withColumnRenamed('count(1)', 'count').head(), expected_agg)

.. notebook:: .. etl/sample-qc-demo.html
