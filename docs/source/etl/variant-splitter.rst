===============================
Splitting Multiallelic Variants
===============================

.. invisible-code-block: python

    import glow
    glow.register(spark)

    test_dir = 'test-data/variantsplitternormalizer-test/'
    df_original = spark.read.format('vcf').load(test_dir + 'test_left_align_hg38_altered.vcf')
    ref_genome_path = test_dir + 'Homo_sapiens_assembly38.20.21_altered.fasta'

**Splitting multiallelic variants to biallelic variants** is a transformation sometimes required before further downstream analysis. Glow provides the ``split_multiallelics" transformer to be appied on a varaint DataFrame to split multiallelic variants in the DataFrame to biallelic variants.

.. note::

  The splitting logic is the same as the one used in GATK's `LeftAlignAndTrimVariants <https://software.broadinstitute.org/gatk/documentation/tooldocs/3.8-0/org_broadinstitute_gatk_tools_walkers_variantutils_LeftAlignAndTrimVariants.php>`_ tool using ``--splitMultiallelics`` option. In splitting a multiallelic variant, this transformer recalculates the GT blocks for the resulting biallelic variants if possible, and drops all VCF INFO fields, except for AC, AN, and AF, which are recalculated based on the newly calculated GT blocks, if any, and otherwise dropped.

Usage
=====

Assuming ``df_original`` is a variable of type DataFrame which contains the genomic variant records, and ``ref_genome_path`` is a variable of type String containing the path to the reference genome file, a minimal example of using this transformer for normalization is:

.. tabs::

    .. tab:: Python

        .. code-block:: python

            df_normalized = glow.transform("normalize_variants", df_original, reference_genome_path=ref_genome_path)

        .. invisible-code-block: python

            from pyspark.sql import Row

            expected_normalized_variant = Row(contigName='chr20', start=268, end=269, names=[], referenceAllele='A', alternateAlleles=['ATTTGAGATCTTCCCTCTTTTCTAATATAAACACATAAAGCTCTGTTTCCTTCTAGGTAACTGG'], qual=30.0, filters=[], splitFromMultiAllelic=False, INFO_AN=4, INFO_AF=[1.0], INFO_AC=[1], genotypes=[Row(sampleId='CHMI_CHMI3_WGS2', alleleDepths=None, phased=False, calls=[1, 1]), Row(sampleId='CHMI_CHMI3_WGS3', alleleDepths=None, phased=False, calls=[1, 1])])
            assert rows_equal(df_normalized.head(), expected_normalized_variant)

    .. tab:: Scala

        .. code-block:: scala

            df_normalized = Glow.transform("normalize_variants", df_original, Map("reference_genome_path" -> ref_genome_path))

Options
=======
The ``normalize_variants`` transformer has the following options:

.. list-table::
   :header-rows: 1

   * - Option
     - Type
     - Possible values and description
   * - ``mode``
     - string
     - | ``normalize``: Only normalizes the variants (if user does not pass the option, ``normalize`` is assumed as default)
       | ``split_and_normalize``: Split multiallelic variants to biallelic variants and normalize them
       | ``split``: Only split the multiallelic variants to biallelic without normalizing
   * - ``referenceGenomePath``
     - string
     - Path to the reference genome ``.fasta`` or ``.fa`` file (required for normalization)

       **Notes**:

       * ``.fai`` and ``.dict`` files with the same name must be present in the same folder.
       * This option is not required for the ``split`` mode as the reference genome is only used for normalization.


.. notebook:: .. etl/normalizevariants-transformer.html
  :title: Variant normalization notebook
