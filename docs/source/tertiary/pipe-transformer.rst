=========================================================================
Parallelizing Command-Line Bioinformatics Tools With the Pipe Transformer
=========================================================================

.. invisible-code-block: python

    import glow
    glow.register(spark)

    path = 'test-data/NA12878_21_10002403.vcf'
    bed = 'test-data/bedtools/intersect_21.bed'

To use single-node tools on massive data sets, Glow includes a
utility called the Pipe Transformer to process Spark DataFrames with command-line tools.

Usage
=====

Consider a minimal case with a DataFrame containing a single column of strings. You can use the Pipe
Transformer to reverse each of the strings in the input DataFrame using the ``rev`` Linux command:

.. tabs::

    .. tab:: Python

        Provide options through the ``arg_map`` argument or as keyword args.

        .. code-block:: python

            # Create a text-only DataFrame
            df = spark.createDataFrame([['foo'], ['bar'], ['baz']], ['value'])
            rev_df = glow.transform('pipe', df, cmd=['rev'], input_formatter='text', output_formatter='text')

        .. invisible-code-block: python

           assert rev_df.head().text == 'oof'

    .. tab:: Scala

        Provide options as a ``Map[String, Any]``.

        .. code-block:: scala

            Glow.transform("pipe", df, Map(
                "cmd" -> Seq("grep", "-v", "#INFO"),
                "inputFormatter" -> "vcf",
                "outputFormatter" -> "vcf",
                "inVcfHeader" -> "infer")
            )

The options in this example demonstrate how to control the basic behavior of the transformer:

  - ``cmd`` is a JSON-encoded array that contains the command to invoke the program
  - ``input_formatter`` defines how each input row should be passed to the program
  - ``output_formatter`` defines how the program output should be converted into a new DataFrame

The input DataFrame can come from any Spark data source --- Delta, Parquet, VCF, BGEN, and so on.

Integrating with bioinformatics tools
=====================================

To integrate with tools for genomic data, you can configure the Pipe Transformer to write each
partition of the input DataFrame as VCF by choosing ``vcf`` as the input and output formatter.
Here is an example using bedtools, note that the bioinformatics tool must be installed on each
virtual machine of the Spark cluster.

.. code-block:: python

    df = spark.read.format("vcf").load(path)

    intersection_df = glow.transform(
        'pipe',
        df,
        cmd=['bedtools', 'intersect', '-a', 'stdin', '-b', bed, '-header', '-wa'],
        input_formatter='vcf',
        in_vcf_header='infer',
        output_formatter='vcf'
    )

.. invisible-code-block: python

   from pyspark.sql import Row
   intersection_rows = intersection_df.select("contigName", "start").collect()
   assert(len(intersection_rows) == 2)
   assert_rows_equal(intersection_rows[0], Row(contigName="21", start=10002402))
   assert_rows_equal(intersection_rows[1], Row(contigName="21", start=10002453))

You must specify a method to determine the VCF header when using the `VCF input formatter`_.
The option ``infer`` instructs the Pipe Transformer to derive a VCF header from the DataFrame schema.
Alternately, you can provide the header as a blob, or you can point to the filesystem path for an existing VCF file with
the correct header.

.. _transformer-options:

Options
=======

Option keys and values are always strings. You can specify option names in snake or camel case; for example
``inputFormatter``, ``input_formatter``, and ``InputFormatter`` are all equivalent.

.. list-table::
  :header-rows: 1

  * - Option
    - Description
  * - ``cmd``
    - The command, specified as an array of strings, to invoke the piped program. The program's stdin
      receives the formatted contents of the input DataFrame, and the output DataFrame is
      constructed from its stdout. The stderr stream will appear in the executor logs.
  * - ``input_formatter``
    - Converts the input DataFrame to a format that the piped program understands. Built-in
      input formatters are ``text``, ``csv``, and ``vcf``.
  * - ``output_formatter``
    - Converts the output of the piped program back into a DataFrame. Built-in output
      formatters are ``text``, ``csv``, and ``vcf``.
  * - ``env_*``
    - Options beginning with ``env_`` are interpreted as environment variables. Like other options,
      the environment variable name is converted to lower snake case. For example,
      providing the option ``env_aniMal=MONKEY`` results in an environment variable with key
      ``ani_mal`` and value ``MONKEY`` being provided to the piped program.

Some of the input and output formatters take additional options.

VCF input formatter
-------------------

.. list-table::
  :header-rows: 1

  * - Option
    - Description
  * - ``in_vcf_header``
    - How to determine a VCF header from the input DataFrame. Possible values:

      * ``infer``: Derive a VCF header from the DataFrame schema. The inference behavior matches that of the
        :ref:`sharded VCF writer <infer-vcf-samples>`.
      * The complete contents of a VCF header starting with ``##``
      * A Hadoop filesystem path to a VCF file. The header from this file is used as the VCF header for each partition.

The CSV input and output formatters accept most of the same options as the CSV data source.
You must prefix options to the input formatter with ``in_``, and options to the output formatter with ``out_``. For
example, ``in_quote`` sets the quote character when writing the input DataFrame to the piped program.

The following options are not supported:

 - ``path`` options are ignored
 - The ``parserLib`` option is ignored. ``univocity`` is always used as the CSV parsing library.

Cleanup
=======

The pipe transformer uses RDD caching to optimize performance. Spark automatically drops old data partitions in a
least-recently-used (LRU) fashion. If you would like to manually clean up the RDDs cached by the pipe transformer
instead of waiting for them to fall out of the cache, use the pipe cleanup transformer on any DataFrame. Do not perform
cleanup until the pipe transformer results have been materialized, such as by being written to a
`Delta Lake table <https://delta.io>`_.

.. tabs::

    .. tab:: Python

        .. code-block:: py

            glow.transform('pipe_cleanup', df)

    .. tab:: Scala

        .. code-block:: scala

            Glow.transform("pipe_cleanup", df)

.. notebook:: .. tertiary/pipe-transformer.html
