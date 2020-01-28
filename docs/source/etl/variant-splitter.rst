.. _split_multiallelics:

===============================
Split Multiallelic Variants
===============================

.. invisible-code-block: python

    import glow
    glow.register(spark)

    test_dir = 'test-data/variantsplitternormalizer-test/'
    df_original = spark.read.format('vcf').load(test_dir + '01_IN_altered_multiallelic.vcf')

**Splitting multiallelic variants to biallelic variants** is a transformation sometimes required before further downstream analysis. Glow provides the ``split_multiallelics`` transformer to be applied on a varaint DataFrame to split multiallelic variants in the DataFrame to biallelic variants. This transformer is able to handle any number of ``ALT`` alleles and any ploidy.

.. note::

    The splitting logic used by the ``split_multiallelics`` transformer is the same as the one used by the `vt decompose tool <https://genome.sph.umich.edu/wiki/Vt#Decompose>`_ of the vt package with option ``-s`` (note that the example provided at `vt decompose user manual page <https://genome.sph.umich.edu/wiki/Vt#Decompose>`_ does not reflect the behavior of ``vt decompose -s`` completely correctly).

    The precise behavior of the ``split_multiallelics`` transformer is presented below:

    - A given multiallelic row with :math:`n` ``ALT`` alleles is split to :math:`n` biallelic rows, each with one of the ``ALT`` alleles of the original multiallelic row. The ``REF`` allele in all split rows is the same as the ``REF`` allele in the multiallelic row.

    - Each ``INFO`` field is appropriately split among split rows if it has the same number of elements as number of ``ALT`` alleles, otherwise it is repeated in all split rows. The boolean ``INFO`` field ``splitFromMultiAllelic`` is added/modified to reflect whether the new row is the result of splitting a multiallelic row through this transformation or not. A new ``INFO`` field called ``OLD_MULTIALLELIC`` is added to the DataFrame, which for each split row, holds the ``CHROM:POS:REF/ALT`` of its original multiallelic row. Note that the ``INFO`` field must be flattened (as explained :ref:`here<vcf>`) in order to be split by this transformer. Unflattened ``INFO`` fields (such as those inside an ``attributes`` field) will not be split, but just repeated in whole across all split rows.

    - Genotype fields for each sample are treated as follows: The ``GT`` field becomes biallelic in each row, where the original ``ALT`` alleles that are not present in that row are replaced with no call. The fields with number of entries equal to number of ``REF`` + ``ALT`` alleles, are properly split into rows, where in each split row, only entries corresponding to the ``REF`` allele as well as the ``ALT`` allele present in that row are kept. The fields which follow colex order (e.g., ``GL``, ``PL``, and ``GP``) are properly split between split rows where in each row only the elements corresponding to genotypes comprising of the ``REF`` and ``ALT`` alleles in that row are listed. Other genotype fields are just repeated over the split rows.

    - Any other field in the DataFrame is just repeated across the split rows.

    As an example (shown in VCF file format), the following multiallelic row

    .. code-block::

        #CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
        20	101	.	A	ACCA,TCGG	.	PASS	VC=INDEL;AC=3,2;AF=0.375,0.25;AN=8	GT:AD:DP:GQ:PL	0/1:2,15,31:30:99:2407,0,533,697,822,574

    will be split into the following two biallelic rows:

    .. code-block::

        #CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	SAMPLE1
        20	101	.	A	ACCA	.	PASS	VC=INDEL;AC=3;AF=0.375;AN=8;OLD_MULTIALLELIC=20:101:A/ACCA/TCGG	GT:AD:DP:GQ:PL	0/1:2,15:30:99:2407,0,533
        20	101	.	A	TCGG	.	PASS	VC=INDEL;AC=2;AF=0.25;AN=8;OLD_MULTIALLELIC=20:101:A/ACCA/TCGG	GT:AD:DP:GQ:PL	0/.:2,31:30:99:2407,697,574


Usage
=====

Assuming ``df_original`` is a variable of type DataFrame which contains the genomic variant records, an example of using this transformer for splitting multiallelic variants is:

.. tabs::

    .. tab:: Python

        .. code-block:: python

            df_split = glow.transform("split_multiallelics", df_original)

        .. invisible-code-block: python

            from pyspark.sql import Row

            expected_split_variant = Row(contigName='20', start=100, end=101, names=[], referenceAllele='A', alternateAlleles=['ACCA'], qual=None, filters=['PASS'], splitFromMultiAllelic=True, INFO_VC='INDEL', INFO_AC=[3], INFO_AF=[0.375], INFO_AN=8, **{'INFO_refseq.name':'NM_144628', 'INFO_refseq.positionType':'intron'},INFO_OLD_MULTIALLELIC='20:101:A/ACCA/TCGG', genotypes=[Row(sampleId='SAMPLE1',  calls=[0, 1], alleleDepths=[2,15], phased=False, depth=30, conditionalQuality=99, phredLikelihoods=[2407,0,533]), Row(sampleId='SAMPLE2', calls=[1, -1], alleleDepths=[2,15], phased=False, depth=30, conditionalQuality=99, phredLikelihoods=[2407,585,533]), Row(sampleId='SAMPLE3',  calls=[0, 1], alleleDepths=[2,15], phased=False, depth=30, conditionalQuality=99, phredLikelihoods=[2407,0,533]), Row(sampleId='SAMPLE4',  calls=[0, -1], alleleDepths=[2,15], phased=False, depth=30, conditionalQuality=99, phredLikelihoods=[2407,822,533])])
            assert_rows_equal(df_split.head(), expected_split_variant)

    .. tab:: Scala

        .. code-block:: scala

            df_split = Glow.transform("split_multiallelics", df_original)

.. tip::

    The ``split_multiallelics`` transformer is often significantly faster if the `whole-stage code generation` feature of Spark Sql is turned off. Therefore, it is recommended that you temporarily turn off this feature using the following command before using this transformer.

    .. tabs::

       .. tab:: Python

            .. code-block:: python

              spark.conf.set("spark.sql.codegen.wholeStage", False)

       .. tab:: Scala

            .. code-block:: scala

              spark.conf.set("spark.sql.codegen.wholeStage", false)

    Remember to turn this feature back on after your split DataFrame is materialized.

.. notebook:: .. etl/splitmultiallelics-transformer.html
  :title: Split Multiallelic Variants notebook
