Python API
==========

Glow's Python API is designed to work seamlessly with PySpark and other tools in the Spark
ecosystem. The methods and functions here work with normal PySpark DataFrames and columns.

You can access methods in any module from the top-level ``glow`` import.

For example:

.. code-block:: python

  import glow
  glow.transform('pipe', df, ....)
  df.select(glow.linear_regression_gwas(...))

.. toctree::

  functions

.. automodule:: glow.glow
  :members:
