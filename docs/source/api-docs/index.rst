Python API
==========

Glow's Python API is designed to work seamlessly with PySpark and other tools in the Spark ecosystem. The functions here work with normal PySpark DataFrames and columns. You can access functions in any module from the top-level ``glow`` import.

Glow Registerer and Transformer Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: glow.glow
  :members:


Glow PySpark Functions
~~~~~~~~~~~~~~~~~~~~~~

Glow includes a number of functions that operate on PySpark columns. These functions are
interoperable with functions provided by PySpark or other libraries.

.. automodule:: glow.functions
  :members:

