=========
Liftover
=========

Liftover tools convert genomic data between reference assemblies. The `UCSC liftOver tool`_  uses a `chain file`_ to
perform simple coordinate conversion, for example on `BED files`_. The `Picard LiftOverVcf tool`_ also uses the new
`reference assembly file`_ to transform variant information (eg. alleles and INFO fields).
Glow can be used to run `coordinate liftover`_ and `variant liftover`_.

.. _`UCSC liftOver tool`: https://genome.ucsc.edu/cgi-bin/hgLiftOver
.. _`chain file`: https://genome.ucsc.edu/goldenPath/help/chain.html
.. _`reference assembly file`: https://software.broadinstitute.org/gatk/documentation/article?id=11013
.. _`BED files`: https://genome.ucsc.edu/FAQ/FAQformat.html#format1
.. _`Picard LiftOverVcf tool`: https://software.broadinstitute.org/gatk/documentation/tooldocs/current/picard_vcf_LiftoverVcf.php

Create a liftover cluster
==========================

For both coordinate and variant liftover, you need a chain file on every node of the cluster. The following example downloads a chain file for liftover
from the b37 to the hg38 reference assembly.

.. code-block:: bash

    #!/usr/bin/env bash
    set -ex
    set -o pipefail
    mkdir /opt/liftover
    curl https://raw.githubusercontent.com/broadinstitute/gatk/master/scripts/funcotator/data_sources/gnomAD/b37ToHg38.over.chain --output /opt/liftover/b37ToHg38.over.chain

Coordinate liftover
====================

To perform liftover for genomic coordinates, use the function ``lift_over_coordinates``. ``lift_over_coordinates`` has
the following parameters.

- chromosome: ``string``
- start: ``long``
- end: ``long``
- chain file: ``string`` (constant value, such as one created with ``lit()``)
- minimum fraction of bases that must remap: ``double`` (optional, defaults to ``.95``)

The returned ``struct`` has the following values if liftover succeeded. If not, the UDF returns ``null``.

- ``contigName``: ``string``
- ``start``: ``long``
- ``end``: ``long``

.. code-block:: py

    from pyspark.sql.functions import expr
    liftover_expr = "lift_over_coordinates(contigName, start, end, '/opt/liftover/b37ToHg38.over.chain', .99)"
    input_with_lifted_df = input_df.withColumn('lifted', expr(liftover_expr))


Variant liftover
=================

For genetic variant data, use the ``lift_over_variants`` transformer. In addition to performing liftover for genetic
coordinates, variant liftover performs the following transformations:

- Reverse-complement and left-align the variant if needed
- Adjust the SNP, and correct AF-like INFO fields and the relevant genotypes if the reference and alternate alleles have
  been swapped in the new genome build

Pull a target assembly :ref:`reference file <reference-genomes>` down to every node in the Spark cluster in addition to
a chain file before performing variant liftover.

The ``lift_over_variants`` transformer operates on a DataFrame containing genetic variants and supports the following
:ref:`options <transformer-options>`.

.. list-table::
  :header-rows: 1

  * - Parameter
    - Default
    - Description
  * - chain_file
    - n/a
    - The path of the chain file.
  * - reference_file
    - n/a
    - The path of the target reference file.
  * - min_match_ratio
    - .95
    - Minimum fraction of bases that must remap.

The output DataFrame's schema consists of the input DataFrame's schema with the following fields appended:

- ``INFO_SwappedAlleles``: ``boolean`` (null if liftover failed, true if the reference and alternate alleles were
  swapped, false otherwise)
- ``INFO_ReverseComplementedAlleles``: ``boolean`` (null if liftover failed, true if the reference and alternate
  alleles were reverse complemented, false otherwise)
- ``liftOverStatus``: ``struct``

   * ``success``: ``boolean`` (true if liftover succeeded, false otherwise)
   * ``errorMessage``: ``string`` (null if liftover succeeded, message describing reason for liftover failure otherwise)

If liftover succeeds, the output row contains the liftover result and ``liftOverStatus.success`` is true.
If liftover fails, the output row contains the original input row, the additional ``INFO`` fields are null,
``liftOverStatus.success`` is false, and ``liftOverStatus.errorMessage`` contains the reason liftover failed.

.. code-block:: py

    import glow
    chain_file = '/opt/liftover/b37ToHg38.over.chain'
    reference_file = '/mnt/dbnucleus/dbgenomics/grch38/data/GRCh38_full_analysis_set_plus_decoy_hla.fa'
    output_df = glow.transform('lift_over_variants', input_df, chain_file=chain_file, reference_file=reference_file)


.. notebook:: ../_static/notebooks/etl/lift-over.html
  :title: Liftover notebook
