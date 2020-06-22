.. _liftover:

=========
Liftover
=========

.. invisible-code-block: python

    import glow
    glow.register(spark)

    input_df = spark.read.format('vcf').load('test-data/combined.chr20_18210071_18210093.g.vcf')
    chain_file = 'test-data/liftover/hg38ToHg19.over.chain.gz'
    reference_file = 'test-data/liftover/hg19.chr20.fa.gz'

LiftOver converts genomic data between reference assemblies. The `UCSC liftOver tool`_  uses a `chain file`_ to
perform simple coordinate conversion, for example on `BED files`_. The `Picard LiftOverVcf tool`_ also uses the new
`reference assembly file`_ to transform variant information (eg. alleles and INFO fields).
Glow can be used to run `coordinate liftOver`_ and `variant liftOver`_.

.. _`UCSC liftOver tool`: https://genome.ucsc.edu/cgi-bin/hgLiftOver
.. _`chain file`: https://genome.ucsc.edu/goldenPath/help/chain.html
.. _`reference assembly file`: https://gatk.broadinstitute.org/hc/en-us/articles/360035531652?id=11013
.. _`BED files`: https://genome.ucsc.edu/FAQ/FAQformat.html#format1
.. _`Picard LiftOverVcf tool`: https://gatk.broadinstitute.org/hc/en-us/articles/360036857991-LiftoverVcf-Picard

Create a liftOver cluster
==========================

For both coordinate and variant liftOver, you need a chain file on every node of the cluster.
On a Databricks cluster, an example of a
`cluster-scoped init script <https://docs.databricks.com/clusters/init-scripts.html#cluster-scoped-init-scripts>`_
you can use to download the required file for liftOver from the b37 to the hg38 reference assembly is as follows:

.. code-block:: bash

    #!/usr/bin/env bash
    set -ex
    set -o pipefail
    mkdir /opt/liftover
    curl https://raw.githubusercontent.com/broadinstitute/gatk/master/scripts/funcotator/data_sources/gnomAD/b37ToHg38.over.chain --output /opt/liftover/b37ToHg38.over.chain

Coordinate liftOver
====================

To perform liftOver for genomic coordinates, use the function ``lift_over_coordinates``. ``lift_over_coordinates``, which has
the following parameters.

- chromosome: ``string``
- start: ``long``
- end: ``long``
- chain file: ``string`` (constant value, such as one created with ``lit()``)
- minimum fraction of bases that must remap: ``double`` (optional, defaults to ``.95``)

The returned ``struct`` has the following values if liftOver succeeded. If not, the function returns ``null``.

- ``contigName``: ``string``
- ``start``: ``long``
- ``end``: ``long``

.. code-block:: python

    output_df = input_df.withColumn('lifted', glow.lift_over_coordinates('contigName', 'start',
      'end', chain_file, 0.99))

.. invisible-code-block: python

    from pyspark.sql import Row
    assert_rows_equal(output_df.select('lifted').head().lifted, Row(contigName='chr20', start=18190714, end=18190715))

Variant liftOver
=================

For genetic variant data, use the ``lift_over_variants`` transformer. In addition to performing liftOver for genetic
coordinates, variant liftOver performs the following transformations:

- Reverse-complement and left-align the variant if needed
- Adjust the SNP, and correct allele-frequency-like INFO fields and the relevant genotypes if the reference and alternate alleles have
  been swapped in the new genome build

Pull a target assembly reference file down to every node in the Spark cluster in addition to a chain file before
performing variant liftOver.

The ``lift_over_variants`` transformer operates on a DataFrame containing genetic variants and supports the following
options:

.. list-table::
  :header-rows: 1

  * - Parameter
    - Default
    - Description
  * - ``chain_file``
    - n/a
    - The path of the chain file.
  * - ``reference_file``
    - n/a
    - The path of the target reference file.
  * - ``min_match_ratio``
    - .95
    - Minimum fraction of bases that must remap.

The output DataFrame's schema consists of the input DataFrame's schema with the following fields appended:

- ``INFO_SwappedAlleles``: ``boolean`` (null if liftOver failed, true if the reference and alternate alleles were
  swapped, false otherwise)
- ``INFO_ReverseComplementedAlleles``: ``boolean`` (null if liftover failed, true if the reference and alternate
  alleles were reverse complemented, false otherwise)
- ``liftOverStatus``: ``struct``

   * ``success``: ``boolean`` (true if liftOver succeeded, false otherwise)
   * ``errorMessage``: ``string`` (null if liftOver succeeded, message describing reason for liftOver failure otherwise)

If liftOver succeeds, the output row contains the liftOver result and ``liftOverStatus.success`` is true.
If liftOver fails, the output row contains the original input row, the additional ``INFO`` fields are null,
``liftOverStatus.success`` is false, and ``liftOverStatus.errorMessage`` contains the reason liftOver failed.

.. code-block:: python

    output_df = glow.transform('lift_over_variants', input_df, chain_file=chain_file, reference_file=reference_file)

.. invisible-code-block: python

   lifted_variant = output_df.select('contigName', 'start', 'end', 'INFO_SwappedAlleles', 'INFO_ReverseComplementedAlleles', 'liftOverStatus').head()
   expected_variant = Row(contigName='chr20', start=18190714, end=18190715, INFO_SwappedAlleles=None, INFO_ReverseComplementedAlleles=None, liftOverStatus=Row(errorMessage=None, success=True))
   assert_rows_equal(lifted_variant, expected_variant)

.. notebook:: .. etl/lift-over.html
  :title: Liftover notebook
