Troubleshooting
===============

- Job is slow or OOMs (throws an ``OutOfMemoryError``) while using an aggregate like ``collect_list`` or
  ``sample_call_summary_stats``

  * Try disabling the `ObjectHashAggregate
    <https://github.com/apache/spark/commit/27daf6bcde782ed3e0f0d951c90c8040fd47e985>`_ by setting
    ``spark.sql.execution.useObjectHashAggregateExec`` to ``false``

- Job is slow or OOMs while writing to partitioned table

  * This error can occur when reading from highly compressed files. Try decreasing
    ``spark.files.maxPartitionBytes`` to a smaller value like ``33554432`` (32MB)

- My VCF looks weird after merging VCFs and saving with ``bigvcf``

  * When saving to a VCF, the samples in the genotypes array must be in the same order for each row.
    This ordering is not guaranteed when using ``collect_list`` to join multiple VCFs. Try sorting
    the array using ``sort_array``.

- Glow's behavior changed after a release

  * See the Glow `release notes <https://github.com/projectglow/glow/releases>`_. If the Glow release
    involved a Spark version change, see the
    `Spark migration guide <https://spark.apache.org/docs/latest/migration-guide.html>`_.
