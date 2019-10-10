==========================
Variant I/O with Spark SQL
==========================

Glow includes Spark SQL support for reading and writing variant data in parallel directly from S3.

.. tip::

  This topic uses the terms "variant" or "variant data" to refer to
  single nucleotide variants and short indels.

.. _vcf:

VCF
===

You can use Spark to read VCF files just like any other file format that Spark supports through
the DataFrame API using Python, R, Scala, or SQL.

.. code-block:: py

  df = spark.read.format("com.databricks.vcf").load(path)

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

+----------------------+---------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| Parameter            | Type    | Default | Description                                                                                                                                             |
+======================+=========+=========+=========================================================================================================================================================+
| asADAMVariantContext | boolean | false   | If true, rows are emitted in the VariantContext schema from the `ADAM <https://github.com/bigdatagenomics/adam>`_ project.                              |
+----------------------+---------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| includeSampleIds     | boolean | true    | If true, each genotype includes the name of the sample ID it belongs to. Sample names increases the size of each row, both in memory and on storage.    |
+----------------------+---------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| splitToBiallelic     | boolean | false   | If true, multiallelic variants are split into two or more biallelic variants.                                                                           |
+----------------------+---------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------+
| flattenInfoFields    | boolean | true    | If true, each info field in the input VCF will be converted into a column in the output DataFrame with each column typed as specified in the VCF header.|
|                      |         |         | If false, all info fields will be contained in a single column with a string -> string map of info keys to values.                                      |
+----------------------+---------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------+

.. important:: The VCF reader uses the 0-start, half-open (zero-based) coordinate system.

You can save a DataFrame as a VCF file, which you can then read with other tools. To write a DataFrame as a single VCF file specify the format ``"com.databricks.bigvcf"``:

.. code-block:: py

  df.write.format("com.databricks.bigvcf").save(<path-to-file>)

The file extension of the output path determines which, if any, compression codec should be used.
For instance, writing to a path such as ``/genomics/my_vcf.vcf.bgz`` will cause the output file to be
block gzipped.

If you'd rather save a sharded VCF where each partition saves to a separate file:

.. code-block:: py

  df.write.format("com.databricks.vcf").save(path)

To control the behavior of the sharded VCF writer, you can provide the following option:

+-------------+--------+---------+--------------------------------------------------------------------------------------------------------------------+
| Parameter   | Type   | Default | Description                                                                                                        |
+=============+========+=========+====================================================================================================================+
| compression | string | n/a     | A compression codec to use for the output VCF file. The value can be the full name of a compression codec class    |
|             |        |         | (for example ``GzipCodec``) or a short alias (for example ``gzip``). To use the block gzip codec, specify ``bgzf``.|
+-------------+--------+---------+--------------------------------------------------------------------------------------------------------------------+

For both the single and sharded VCF writer, you can use the following option to determine the header:

+-------------+--------+---------+--------------------------------------------------------------------------------------------------------------------+
| Parameter   | Type   | Default | Description                                                                                                        |
+=============+========+=========+====================================================================================================================+
| vcfHeader   | string | infer   | If ``infer``, infers the header from the DataFrame schema. This value can be a complete header                     |
|             |        |         | starting with ``##`` or a Hadoop filesystem path (for example, ``dbfs://...``) to a VCF file. The header from      |
|             |        |         | this file is used as the VCF header for each partition.                                                            |
+-------------+--------+---------+--------------------------------------------------------------------------------------------------------------------+


BGEN
====

Glow also provides the ability to read BGEN files, including those distributed by the UK Biobank project.

.. code-block:: py

  df = spark.read.format("com.databricks.bgen").load(path)

As with the VCF reader, the provided path can be a file, directory, or glob pattern. If ``.bgi``
index files are located in the same directory as the data files, the reader uses the indexes to
more efficiently traverse the data files. Data files can be processed even if indexes do not exist.
The schema of the resulting DataFrame matches that of the VCF reader.

+----------------+---------+---------+------------------------------------------------------------------------------------------------------------+
| Parameter      | Type    | Default | Description                                                                                                |
+================+=========+=========+============================================================================================================+
| useBgenIndex   | boolean | true    | If true, use ``.bgi`` index files.                                                                         |
+----------------+---------+---------+------------------------------------------------------------------------------------------------------------+
| sampleFilePath | string  | n/a     | Path to a ``.sample`` Oxford sample information file containing sample IDs if not stored in the BGEN file. |
+----------------+---------+---------+------------------------------------------------------------------------------------------------------------+
| sampleIdColumn | string  | ID_2    | Name of the column in the ``.sample`` file corresponding to the sample IDs.                                |
+----------------+---------+---------+------------------------------------------------------------------------------------------------------------+

You can use the ``DataFrameWriter`` API to save a single BGEN file, which you can then read with other tools.

.. code-block:: py

  df.write.format("com.databricks.bigbgen").save(path)

If the genotype arrays are missing ploidy and/or phasing information, the BGEN writer infers the values using the
provided values for ploidy, phasing, or ``posteriorProbabilities`` in the genotype arrays. You can provide the value for ploidy
using an integer value ``ploidy`` or it can be inferred using the length of an array ``calls``, and you can provide the phasing information
using a boolean value ``phased``.

To control the behavior of the BGEN writer, you can provide the following options:

+------------------------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------+
| Parameter              | Type    | Default | Description                                                                                                                        |
+========================+=========+=========+====================================================================================================================================+
| bitsPerProbability     | integer | 16      | Number of bits used to represent each probability value. Must be 8, 16, or 32.                                                     |
+------------------------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------+
| maximumInferredPloidy  | integer | 10      | The maximum ploidy that will be inferred for unphased data if ploidy is missing.                                                   |
+------------------------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------+
| defaultInferredPloidy  | integer | 2       | The inferred ploidy if phasing and ploidy are missing, or ploidy is missing and cannot be inferred from ``posteriorProbabilities``.|
+------------------------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------+
| defaultInferredPhasing | boolean | false   | The inferred phasing if phasing is missing and cannot be inferred from ``posteriorProbabilities``.                                 |
+------------------------+---------+---------+------------------------------------------------------------------------------------------------------------------------------------+

.. notebook:: ../_static/notebooks/variant-data.html
