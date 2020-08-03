.. _variant-qc:

=======================
Variant Quality Control
=======================

Glow includes a variety of tools for variant quality control.

.. tip::

  This topic uses the terms "variant" or "variant data" to refer to
  single nucleotide variants and short indels.

You can calculate quality control statistics on your variant data using Spark SQL functions, which can be expressed in Python, R, Scala, or SQL.

.. list-table::
  :header-rows: 1

  * - Function
    - Arguments
    - Return
  * - ``hardy_weinberg``
    - The ``genotypes`` array. This function assumes that the variant has been converted to a biallelic representation.
    - A struct with two elements: the expected heterozygous frequency according to Hardy-Weinberg equilibrium and the associated p-value.
  * - ``call_summary_stats``
    - The ``genotypes`` array
    - A struct containing the following summary stats:

      * ``callRate``: The fraction of samples with a called genotype
      * ``nCalled``: The number of samples with a called genotype
      * ``nUncalled``: The number of samples with a missing or uncalled genotype, as represented by a '.' in a VCF or -1 in a DataFrame.
      * ``nHet``: The number of heterozygous samples
      * ``nHomozygous``: An array with the number of samples that are homozygous for each allele. The 0th element describes how many sample are hom-ref.
      * ``nNonRef``: The number of samples that are not hom-ref
      * ``nAllelesCalled``: An array with the number of times each allele was seen
      * ``alleleFrequencies``: An array with the frequency for each allele
  * - ``dp_summary_stats``
    - The ``genotypes`` array
    - A struct containing the min, max, mean, and population standard deviation for genotype depth (DP in VCF v4.2 specificiation) across all samples
  * - ``gq_summary_stats``
    - The ``genotypes`` array
    - A struct containing the min, max, mean, and population standard deviation for genotype quality (GQ in VCF v4.2 specification) across all samples

.. notebook:: .. etl/variant-qc-demo.html
