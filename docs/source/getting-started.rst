Getting Started
===============

Running Locally
---------------

Glow requires Apache Spark 2.4.2 or above. If you don't have a local Apache Spark installation,
you can install it from PyPI:

.. code-block:: sh
  
  pip install pyspark==2.4.2

or `download a specific distribution <https://spark.apache.org/downloads.html>`_.

Install the Python frontend from pip:

.. code-block:: sh
  
  pip install glow.py

and then start the `Spark shell <http://spark.apache.org/docs/latest/rdd-programming-guide.html#using-the-shell>`_ 
with the Glow maven package:

.. code-block:: sh

  ./bin/pyspark --packages io.projectglow:glow_2.11:0.1.0

To start a Jupyter notebook instead of a shell:

.. code-block:: sh
  
  PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS=notebook ./bin/pyspark --packages io.projectglow:glow_2.11:0.1.0
  
And now your notebook is glowing! To access the Glow functions, you need to register them with the
Spark session.

.. code-block:: python
  
  import glow
  glow.register(spark)
  df = spark.read.format('vcf').load('my_first.vcf')

Running in the cloud
--------------------

The easiest way to use Glow in the cloud is with the `Databricks Runtime for Genomics
<https://docs.databricks.com/runtime/genomicsruntime.html>`_. However, it works with any cloud
provider or Spark distribution. You need to install the maven package
``io.project:glow_2.11:${version}`` and optionally the Python frontend ``glow.py``.

Example notebook
----------------

This notebook demonstrates some of the key functionality of Glow, like reading in a genomic dataset,
saving it as a `Delta Lake <https://delta.io>`_, and performing a genome-wide assocation study.

.. notebook:: _static/notebooks/tertiary/gwas.html
