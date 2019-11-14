==========================================================
Parallelizing Command-Line Tools With the Pipe Transformer
==========================================================

To use single-node tools on massive data sets, Glow includes a
utility called the Pipe Transformer to process Spark DataFrames with command-line programs.

Python usage
============

.. note::

  The Pipe Transformer requires Python 3.

Consider a minimal case with a DataFrame containing a single column of strings. You can use the Pipe
Transformer to reverse each of the strings in the input DataFrame using the ``rev`` Linux command:

.. code-block:: py

  import glow

  # Create a text-only DataFrame
  df = spark.createDataFrame([['foo'], ['bar'], ['baz']], ['value'])
  display(glow.transform('pipe', df, cmd='["rev"]', inputformatter='text', outputformatter='text'))

The options in this example demonstrate how to control the basic behavior of the transformer:

  - ``cmd`` is a JSON-encoded array that contains the command to invoke the program
  - ``inputformatter`` defines how each input row should be passed to the program
  - ``outputformatter`` defines how the program output should be converted into a new DataFrame

The input DataFrame can come from any Spark data source --- Delta, Parquet, VCF, BGEN, and so on.

Integrating with bioinformatics tools
=====================================

To integrate with tools for genomic data, you can configure the Pipe Transformer to write each
partition of the input DataFrame as VCF by choosing ``vcf`` as the input and output formatter.

.. code-block:: py

  import glow

  df = spark.read.format("vcf")\
    .load("/databricks-datasets/genomics/1kg-vcfs")\
    .limit(1000)

  glow.transform(
    'pipe',
    df,
    cmd='["grep", "-v", "#INFO"]',
    inputformatter='vcf',
    in_vcfheader='infer',
    outputformatter='vcf'
  )

When you use the VCF input formatter, you must specify a method to determine the VCF header. The
simplest option is ``infer``, which instructs the Pipe Transformer to derive a VCF header from the
DataFrame schema.

Scala usage
===========

You can also invoke the Pipe Transformer from Scala. You specify options as a ``Map[String,
String]``.

.. code-block:: scala

  import io.projectglow.Glow

  Glow.transform("pipe", df, Map(
    "cmd" -> "[\"grep\", \"-v\", \"#INFO\"]"
    "inputFormatter" -> "vcf",
    "outputFormatter" -> "vcf",
    "in_vcfHeader" -> "infer")
  )

.. _transformer-options:

Options
=======

Option keys and values are always strings. From Python, you provide options through the ``arg_map``
argument or as keyword args. From Scala, you provide options as a ``Map[String, String]``.
Option names are case insensitive; for example ``inputFormatter``, ``inputformatter``, and ``INPUTFORMATTER`` are all equivalent.

.. list-table::
  :header-rows: 1

  * - Option
    - Description
  * - cmd
    - The command, specified as an array of strings, to invoke the piped program. The program's stdin
      receives the formatted contents of the input DataFrame, and the output DataFrame is
      constructed from its stdout. The stderr stream will appear in the executor logs.
  * - inputFormatter
    - Converts the input DataFrame to a format that the piped program understands. Built-in
      input formatters are ``text``, ``csv``, and ``vcf``.
  * - outputFormatter
    - Converts the output of the piped program back into a DataFrame. Built-in output
      formatters are ``text``, ``csv``, and ``vcf``.
  * - env_*
    - Options beginning with ``env_`` are interpreted as environment variables. For example,
      providing the option ``env_ANIMAL=MONKEY`` results in an environment variable with key
      ``ANIMAL`` and value ``MONKEY`` being provided to the piped program.

Some of the input and output formatters take additional options.

VCF input formatter:

.. list-table::
  :header-rows: 1

  * - Option
    - Description
  * - in_vcfheader
    - How to determine a VCF header from the input DataFrame. Possible values:

      * ``infer``: Derive a VCF header from the DataFrame schema.
      * The complete contents of a VCF header starting with ``##``
      * A Hadoop filesystem path to a VCF file. The header from this file is used as the VCF header for each partition.

The CSV input and output formatters accept most of the same options as the CSV data source.
You must prefix options to the input formatter with ``in_``, and options to the output formatter with ``out_``. For example, ``in_quote`` sets the quote character when writing the input DataFrame to the piped program.

The following options are not supported:

 - ``path`` options are ignored
 - The ``parserLib`` option is ignored. ``univocity`` is always used as the CSV parsing library.

Cleanup
=======

The pipe transformer uses RDD caching to optimize performance. Spark automatically drops old data partitions in a
least-recently-used (LRU) fashion. If you would like to manually clean up the RDDs cached by the pipe transformer
instead of waiting for them to fall out of the cache, use the pipe cleanup transformer on any DataFrame. Do not perform
cleanup until the pipe transformer results have been materialized, such as by being written to Delta.

To perform pipe cleanup in Python, run ``glow.transform('pipe_cleanup', df)``.
In Scala, run ``Glow.transform("pipe_cleanup", df)``.

.. notebook:: .. tertiary/pipe-transformer.html
