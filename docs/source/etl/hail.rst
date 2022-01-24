===================
Hail Interoperation
===================

.. invisible-code-block: python

    import glow
    import hail as hl
    hl.init(spark.sparkContext, idempotent=True, quiet=True)

    vcf = 'test-data/NA12878_21_10002403.vcf'
    mt = hl.import_vcf(vcf)

Glow includes functionality to enable conversion between a
`Hail MatrixTable <https://hail.is/docs/0.2/overview/matrix_table.html>`_ and a Spark DataFrame, similar to one created
with the :ref:`native Glow datasources <variant_data>`.

Create a Hail cluster
=====================

To use the Hail interoperation functions, you need Hail to be installed on the cluster.
On a Databricks cluster,
`install Hail with an environment variable <https://docs.databricks.com/applications/genomics/tertiary-analytics/hail.html#create-a-cluster>`_.
See the `Hail installation documentation <https://hail.is/docs/0.2/getting_started.html>`_ to install Hail in other setups.

Convert to a Glow DataFrame
===========================

Convert from a Hail MatrixTable to a Glow-compatible DataFrame with the function ``from_matrix_table``.

.. code-block:: python

    from glow.hail import functions
    df = functions.from_matrix_table(mt, include_sample_ids=True)

.. invisible-code-block: python

    from pyspark.sql import Row
    native_glow_df = spark.read.format('vcf').load(vcf).drop('splitFromMultiAllelic')
    assert_rows_equal(df.head(), native_glow_df.head())

By default, the genotypes contain sample IDs. To remove the sample IDs, set the parameter ``include_sample_ids=False``.

Schema mapping
==============

The Glow DataFrame variant fields are derived from the Hail MatrixTable row fields.

.. list-table::
  :header-rows: 1

  * - Required
    - Glow DataFrame variant field
    - Hail MatrixTable row field
  * - Yes
    - ``contigName``
    - ``locus.contig``
  * - Yes
    - ``start``
    - ``locus.position - 1``
  * - Yes
    - ``end``
    - ``info.END`` or ``locus.position - 1 + len(alleles[0])``
  * - Yes
    - ``referenceAllele``
    - ``alleles[0]``
  * - No
    - ``alternateAlleles``
    - ``alleles[1:]``
  * - No
    - ``names``
    - ``[rsid, varid]``
  * - No
    - ``qual``
    - ``qual``
  * - No
    - ``filters``
    - ``filters``
  * - No
    - ``INFO_<ANY_FIELD>``
    - ``info.<ANY_FIELD>``

The Glow DataFrame genotype sample IDs are derived from the Hail MatrixTable column fields.

All of the other Glow DataFrame genotype fields are derived from the Hail MatrixTable entry fields.

.. list-table::
  :header-rows: 1

  * - Glow DataFrame genotype field
    - Hail MatrixTable entry field
  * - ``phased``
    - ``GT.phased``
  * - ``calls``
    - ``GT.alleles``
  * - ``depth``
    - ``DP``
  * - ``filters``
    - ``FT``
  * - ``genotypeLikelihoods``
    - ``GL``
  * - ``phredLikelihoods``
    - ``PL``
  * - ``posteriorProbabilities``
    - ``GP``
  * - ``conditionalQuality``
    - ``GQ``
  * - ``haplotypeQualities``
    - ``HQ``
  * - ``expectedAlleleCounts``
    - ``EC``
  * - ``mappingQuality``
    - ``MQ``
  * - ``alleleDepths``
    - ``AD``
  * - ``<ANY_FIELD>``
    - ``<ANY_FIELD>``

.. notebook:: .. etl/hail-interoperation.html
  :title: Hail interoperation notebook
