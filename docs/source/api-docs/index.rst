Python API
==========

Glow's Python API is designed to work seamlessly with PySpark and other tools in the Spark
ecosystem. The methods and functions here work with normal PySpark DataFrames and columns.

You can access methods in any module from the top-level ``glow`` import.

For example:

.. code-block:: python

  import glow
  glow.register(spark)
  df = spark.read.format('vcf').load('test-data/1kg_sample.vcf')
  glow.transform('pipe', df, cmd='["cat"]', input_formatter='vcf', output_formatter='vcf',
      in_vcf_header='infer')
  df.select(glow.genotype_states('genotypes'))

Modules
~~~~~~~

.. toctree::

  functions

Top-Level Functions
~~~~~~~~~~~~~~~~~~~

.. automodule:: glow.glow
  :members:
