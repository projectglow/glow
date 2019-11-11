Getting Started
===============

Running Locally
---------------

Glow requires Apache Spark 2.4.3 (or a later version of Spark 2.4.x that is built on Scala 2.11). If you don't have a
local Apache Spark installation, you can install it from PyPI:

.. code-block:: sh

  pip install pyspark==2.4.3

or `download a specific distribution <https://spark.apache.org/downloads.html>`_.

Install the Python frontend from pip:

.. code-block:: sh

  pip install glow.py

and then start the `Spark shell <http://spark.apache.org/docs/latest/rdd-programming-guide.html#using-the-shell>`_
with the Glow maven package:

.. substitution-code-block:: sh

  ./bin/pyspark --packages io.projectglow:glow_2.11:|mvn-version|

To start a Jupyter notebook instead of a shell:

.. substitution-code-block:: sh

  PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS=notebook ./bin/pyspark --packages io.projectglow:glow_2.11:|mvn-version|

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

Notebooks embedded in the docs
------------------------------

To demonstrate example use cases of Glow functionalities, most doc pages are accompanied by embedded `Databricks Notebooks <https://docs.databricks.com/notebooks/index.html>`_. Most of the code in these notebooks can be run on Spark and Glow alone, but a few functions such as ``display()`` or ``dbutils()`` are only available on Databricks. See :ref:`dbnotebooks` for more info.

Also note that the path to datasets used as example in these notebooks is usually a folder in ``/databricks-datasets/genomics/`` and should be replaced with the appropriate path based on your own folder structure.

Demo notebook
-----------------

This notebook showcases some of the key functionality of Glow, like reading in a genomic dataset,
saving it as a `Delta Lake <https://delta.io>`_, and performing a genome-wide association study.

.. notebook:: . tertiary/gwas.html
