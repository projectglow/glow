Getting Started
===============

Running Locally
---------------

Glow requires Apache Spark 2.4.3 or later.

.. tabs::

    .. tab:: Python

        If you don't have a local Apache Spark installation, you can install it from PyPI:

        .. code-block:: sh

          pip install pyspark==3.0.0

        or `download a specific distribution <https://spark.apache.org/downloads.html>`_.

        Install the Python frontend from pip:

        .. code-block:: sh

          pip install glow.py

        and then start the `Spark shell <http://spark.apache.org/docs/latest/rdd-programming-guide.html#using-the-shell>`_
        with the Glow maven package:

        .. code-block:: sh
          :substitutions:

          ./bin/pyspark --packages io.projectglow:glow-spark3_2.12:|mvn-version| --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec

        To start a Jupyter notebook instead of a shell:

        .. code-block:: sh
          :substitutions:

          PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS=notebook ./bin/pyspark --packages io.projectglow:glow-spark3_2.12:|mvn-version| --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec

        And now your notebook is glowing! To access the Glow functions, you need to register them with the
        Spark session.

        .. invisible-code-block: python

          path = 'test-data/tabix-test-vcf/NA12878_21_10002403.vcf.gz'

        .. code-block:: python

          import glow
          spark = glow.register(spark)
          df = spark.read.format('vcf').load(path)

    .. tab:: Scala

        If you don't have a local Apache Spark installation,
        `download a specific distribution <https://spark.apache.org/downloads.html>`_.

        Start the `Spark shell <http://spark.apache.org/docs/latest/rdd-programming-guide.html#using-the-shell>`_
        with the Glow maven package:

        .. code-block:: sh
          :substitutions:

          ./bin/spark-shell --packages io.projectglow:glow-spark3_2.12:|mvn-version| --conf spark.hadoop.io.compression.codecs=io.projectglow.sql.util.BGZFCodec

        To access the Glow functions, you need to register them with the Spark session.

        .. code-block:: scala

          import io.projectglow.Glow
          Glow.register(spark)
          val df = spark.read.format("vcf").load(path)


Running in the cloud
--------------------

The easiest way to use Glow in the cloud is with the `Databricks Runtime for Genomics
<https://docs.databricks.com/runtime/genomicsruntime.html>`_. However, it works with any cloud
provider or Spark distribution. You need to install the maven package
``io.project:glow-spark${spark_version}_${scala_version}:${glow_version}`` and optionally the Python frontend ``glow.py``. Also set the Spark configuration
``spark.hadoop.io.compression.codecs`` to ``io.projectglow.sql.util.BGZFCodec`` in order to read and write
BGZF-compressed files.

Notebooks embedded in the docs
------------------------------

To demonstrate example use cases of Glow functionalities, most doc pages are accompanied by embedded `Databricks Notebooks <https://docs.databricks.com/notebooks/index.html>`_. Most of the code in these notebooks can be run on Spark and Glow alone, but a few functions such as ``display()`` or ``dbutils()`` are only available on Databricks. See :ref:`dbnotebooks` for more info.

Also note that the path to datasets used as example in these notebooks is usually a folder in ``/databricks-datasets/genomics/`` and should be replaced with the appropriate path based on your own folder structure.

Demo notebook
-----------------

This notebook showcases some of the key functionality of Glow, like reading in a genomic dataset,
saving it as a `Delta Lake <https://delta.io>`_, and performing a genome-wide association study.

.. notebook:: . tertiary/gwas.html
