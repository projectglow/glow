======================
Sample Quality Control
======================

.. invisible-code-block: python

    import glow
    glow.register(spark)

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

``aggregate_by_index``
~~~~~~~~~~~~~~~~~~~~~~

First, you can aggregate over each sample in a genotypes array using the ``aggregate_by_index``
function.

``aggregate_by_index(array, initial_state, update_function, merge_function, eval_function)``

.. list-table::
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``array``
    - ``array<T>``
    - An ``array``-typed column. There are no requirements on the element datatype. This array is expected to be the same length for each row in the input DataFrame. The output of ``aggregate_by_index`` is an array with the same length as each input row.
  * - ``initial_state``
    - ``U``
    - The initial aggregation state for each sample.
  * - ``update_function``
    - ``<U, T> -> U``
    - A function that returns a new single sample aggregation state given the current aggregation state and a new data element.
  * - ``merge_function``
    - ``<U, U> -> U``
    - A function that combines two single sample aggregation states. This function is necessary since the aggregation is computed in a distributed manner across all nodes in the cluster.
  * - ``eval_function`` (optional)
    - ``U -> V``
    - A function that returns the output for a sample given that sample's aggregation state. This function is optional. If it is not specified, the aggregation state will be returned.

For example, this code snippet uses ``aggregate_by_index`` to compute the mean for each array
position:

.. code-block::
  
  aggregate_by_index(
    array_col,
    (0d, 0l),
    (state, element) -> (state.col1 + element, state.col2 + 1),
    (state1, state2) -> (state1.col1 + state2.col1, state1.col2 + state2.col2),
    state -> state.col1 / state.col2)

.. invisible-code-block: python

  import pyspark.sql.functions as fx
  df = spark.range(1000).withColumn("array_col", fx.expr("transform(array_repeat(0, 1000), (el, idx) -> id + el + idx)"))
  agg_expr = "aggregate_by_index(array_col, (0d, 0l), (state, el) -> (state.col1 + el, state.col2 + 1), (state1, state2) -> (state1.col1 + state2.col1, state1.col2 + state2.col2), state -> state.col1 / state.col2) as mean_by_position"
  agged = df.selectExpr(agg_expr)
  expected_means = agged.head().mean_by_position

  assert expected_means[0] == 499.5
  assert expected_means[-1] == 1498.5

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
  assert rows_equal(agg.withColumnRenamed('count(1)', 'count').head(), expected_agg)

.. notebook:: .. etl/sample-qc-demo.html
