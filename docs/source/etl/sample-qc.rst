======================
Sample Quality Control
======================

You can calculate quality control statistics on your variant data using Spark SQL functions, which
can be expressed in Python, R, Scala, or SQL.

Each of these functions returns a map from sample ID to a struct containing metrics for that sample and assumes that the same samples appear in the same order in each row.

.. list-table::
  :header-rows: 1

  * - Functions
    - Arguments
    - Return
  * - sample_call_summary_stats
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
  * - sample_dp_summary_stats
    - ``genotypes`` array with a ``depth`` field
    - A struct with ``min``, ``max``, ``mean``, and ``stddev``
  * - sample_gq_summary_stats
    - ``genotypes`` array with a ``conditionalQuality`` field
    - A struct with ``min``, ``max``, ``mean``, and ``stddev``

.. notebook:: ../_static/notebooks/etl/sample-qc-demo.html
