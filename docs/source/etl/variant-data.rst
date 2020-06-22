.. _variant_data:

==============================================
Read and Write VCF, Plink, and BGEN with Spark
==============================================

.. invisible-code-block: python

    from pyspark.sql import Row
    import glob
    import os
    import shutil

    import glow
    glow.register(spark)


Glow makes it possible to read and write variant data at scale using Spark SQL.

.. tip::

  This topic uses the terms "variant" or "variant data" to refer to
  single nucleotide variants and short indels.

.. _vcf:

VCF
===

You can use Spark to read VCF files just like any other file format that Spark supports through
the DataFrame API using Python, R, Scala, or SQL.

.. invisible-code-block: python

   path = "test-data/test.chr17.vcf"

.. code-block:: python

   df = spark.read.format("vcf").load(path)

.. invisible-code-block: python

   assert_rows_equal(df.select("contigName", "start").head(), Row(contigName='17', start=504217))


The returned DataFrame has a schema that mirrors a single row of a VCF. Information that applies to an entire
variant (SNV or indel), such as the contig name, start and end positions, and INFO attributes,
is contained in columns of the DataFrame. The genotypes, which correspond to the GT FORMAT fields
in a VCF, are contained in an array with one entry per sample.
Each entry is a struct with fields that are described in the VCF header.

The path that you provide
can be the location of a single file, a directory that contains VCF files, or a Hadoop glob pattern
that identifies a group of files. Sample IDs are not included by default. See the
parameters table below for instructions on how to include them.

You can control the behavior of the VCF reader with a few parameters. All parameters are case insensitive.

+--------------------------+---------+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| Parameter                | Type    | Default     | Description                                                                                                                                             |
+==========================+=========+=============+=========================================================================================================================================================+
| ``includeSampleIds``     | boolean | ``true``    | If true, each genotype includes the name of the sample ID it belongs to. Sample names increases the size of each row, both in memory and on storage.    |
+--------------------------+---------+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``flattenInfoFields``    | boolean | ``true``    | If true, each info field in the input VCF will be converted into a column in the output DataFrame with each column typed as specified in the VCF header.|
|                          |         |             | If false, all info fields will be contained in a single column with a string -> string map of info keys to values.                                      |
+--------------------------+---------+-------------+---------------------------------------------------------------------------------------------------------------------------------------------------------+

.. note::

   Starting from Glow 0.4.0, the ``splitToBiallelic`` option for the VCF reader no longer exists. To split multiallelic variants to biallelics use the :ref:`split_multiallelics<split_multiallelics>` transformer after loading the VCF.


.. important:: The VCF reader uses the 0-start, half-open (zero-based) coordinate system.

You can save a DataFrame as a VCF file, which you can then read with other tools. To write a DataFrame as a single VCF file specify the format ``"bigvcf"``:

.. invisible-code-block: python

   base_path = "test-data/doc-test-bigvcf/"
   path = base_path + "test.vcf"

.. code-block:: python

   df.write.format("bigvcf").save(path)

.. invisible-code-block: python

   shutil.rmtree(base_path)

The file extension of the output path determines which, if any, compression codec should be used.
For instance, writing to a path such as ``/genomics/my_vcf.vcf.bgz`` will cause the output file to be
block gzipped.

If you'd rather save a sharded VCF where each partition saves to a separate file:

.. invisible-code-block: python

   path = "test-data/doc-test-vcf.vcf"

.. code-block:: python

   df.write.format("vcf").save(path)

.. invisible-code-block: python

   shutil.rmtree(path)

To control the behavior of the sharded VCF writer, you can provide the following option:

+-----------------+--------+---------+--------------------------------------------------------------------------------------------------------------------+
| Parameter       | Type   | Default | Description                                                                                                        |
+=================+========+=========+====================================================================================================================+
| ``compression`` | string | n/a     | A compression codec to use for the output VCF file. The value can be the full name of a compression codec class    |
|                 |        |         | (for example ``GzipCodec``) or a short alias (for example ``gzip``). To use the block gzip codec, specify ``bgzf``.|
+-----------------+--------+---------+--------------------------------------------------------------------------------------------------------------------+

For both the single and sharded VCF writer, you can use the following option to determine the header:

+-----------------+--------+-------------+--------------------------------------------------------------------------------------------------------------------+
| Parameter       | Type   | Default     | Description                                                                                                        |
+=================+========+=============+====================================================================================================================+
| ``vcfHeader``   | string | ``infer``   | If ``infer``, infers the header from the DataFrame schema. This value can be a complete header                     |
|                 |        |             | starting with ``##`` or a Hadoop filesystem path to a VCF file. The header from                                    |
|                 |        |             | this file is used as the VCF header for each partition.                                                            |
+-----------------+--------+-------------+--------------------------------------------------------------------------------------------------------------------+

.. _infer-vcf-samples:

If the header is inferred from the DataFrame, the sample IDs are derived from the rows. If the sample IDs are missing,
they will be represented as ``sample_n``, for which ``n`` reflects the index of the sample in a row. In this case,
there must be the same number of samples in each row.

- For the big VCF writer, the inferred sample IDs are the distinct set of all sample IDs from the DataFrame.
- For the sharded VCF writer, the sample IDs are inferred from the first row of each partition and must be the same
  for each row. If the rows do not contain the same samples, provide a complete header of a filesystem path to a VCF
  file.

BGEN
====

Glow provides the ability to read BGEN files, including those distributed by the UK Biobank project.

.. invisible-code-block: python

   path = "test-data/bgen/example.8bits.bgen"

.. code-block:: python

   df = spark.read.format("bgen").load(path)

.. invisible-code-block: python

   assert_rows_equal(df.select("contigName", "start").head(), Row(contigName='01', start=1999))

As with the VCF reader, the provided path can be a file, directory, or glob pattern. If ``.bgi``
index files are located in the same directory as the data files, the reader uses the indexes to
more efficiently traverse the data files. Data files can be processed even if indexes do not exist.
The schema of the resulting DataFrame matches that of the VCF reader.

+--------------------+---------+--------------+------------------------------------------------------------------------------------------------------------+
| Parameter          | Type    | Default      | Description                                                                                                |
+====================+=========+==============+============================================================================================================+
| ``useBgenIndex``   | boolean | ``true``     | If true, use ``.bgi`` index files.                                                                         |
+--------------------+---------+--------------+------------------------------------------------------------------------------------------------------------+
| ``sampleFilePath`` | string  | n/a          | Path to a ``.sample`` Oxford sample information file containing sample IDs if not stored in the BGEN file. |
+--------------------+---------+--------------+------------------------------------------------------------------------------------------------------------+
| ``sampleIdColumn`` | string  | ``ID_2``     | Name of the column in the ``.sample`` file corresponding to the sample IDs.                                |
+--------------------+---------+--------------+------------------------------------------------------------------------------------------------------------+

You can use the ``DataFrameWriter`` API to save a single BGEN file, which you can then read with other tools.

.. invisible-code-block: python

   base_path = "test-data/doc-test-bigbgen/"
   path = base_path + "test.bgen"

.. code-block:: python

   df.write.format("bigbgen").save(path)

.. invisible-code-block: python

   shutil.rmtree(base_path)

If the genotype arrays are missing ploidy and/or phasing information, the BGEN writer infers the values using the
provided values for ploidy, phasing, or ``posteriorProbabilities`` in the genotype arrays. You can provide the value for ploidy
using an integer value ``ploidy`` or it can be inferred using the length of an array ``calls``, and you can provide the phasing information
using a boolean value ``phased``.

To control the behavior of the BGEN writer, you can provide the following options:

+-----------------------------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------+
| Parameter                   | Type    | Default     | Description                                                                                                                        |
+=============================+=========+=============+====================================================================================================================================+
| ``bitsPerProbability``      | integer | ``16``      | Number of bits used to represent each probability value. Must be 8, 16, or 32.                                                     |
+-----------------------------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------+
| ``maximumInferredPloidy``   | integer | ``10``      | The maximum ploidy that will be inferred for unphased data if ploidy is missing.                                                   |
+-----------------------------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------+
| ``defaultInferredPloidy``   | integer | ``2``       | The inferred ploidy if phasing and ploidy are missing, or ploidy is missing and cannot be inferred from ``posteriorProbabilities``.|
+-----------------------------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------+
| ``defaultInferredPhasing``  | boolean | ``false``   | The inferred phasing if phasing is missing and cannot be inferred from ``posteriorProbabilities``.                                 |
+-----------------------------+---------+-------------+------------------------------------------------------------------------------------------------------------------------------------+


PLINK
=====

Glow provides the ability to read binary PLINK binary PED (BED) files with accompanying BIM and FAM files. The provided path can be a
file or glob pattern.

.. invisible-code-block: python

   prefix = "test-data/plink/five-samples-five-variants/bed-bim-fam/test"

.. code-block:: python

   df = spark.read.format("plink").load("{prefix}.bed".format(prefix=prefix))

.. invisible-code-block: python

  assert_rows_equal(df.select("contigName", "start").head(), Row(contigName='1', start=9))

The schema of the resulting DataFrame matches that of the VCF reader. The accompanying variant and sample information
files must be located at ``{prefix}.bim`` and ``{prefix}.fam``.

+----------------------+---------+-----------------+-----------------------------------------------------------------------------------------------------+
| Parameter            | Type    | Default         | Description                                                                                         |
+======================+=========+=================+=====================================================================================================+
| ``includeSampleIds`` | boolean | ``true``        | If true, each genotype includes the name of the sample ID it belongs to.                            |
+----------------------+---------+-----------------+-----------------------------------------------------------------------------------------------------+
| ``bimDelimiter``     | string  | (tab)           | Whitespace delimiter in the ``{prefix}.bim`` file.                                                  |
+----------------------+---------+-----------------+-----------------------------------------------------------------------------------------------------+
| ``famDelimiter``     | string  | (space)         | Whitespace delimiter in the ``{prefix}.fam`` file.                                                  |
+----------------------+---------+-----------------+-----------------------------------------------------------------------------------------------------+
| ``mergeFidIid``      | boolean | ``true``        | If true, sets the sample ID to the family ID and individual ID merged with an underscore delimiter. |
|                      |         |                 | If false, sets the sample ID to the individual ID.                                                  |
+----------------------+---------+-----------------+-----------------------------------------------------------------------------------------------------+

.. notebook:: .. etl/variant-data.html
