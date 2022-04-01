Getting Started
===============

Running Locally
---------------

Glow requires Apache Spark 3.2.0.

.. tabs::

    .. tab:: Python

        If you don't have a local Apache Spark installation, you can install it from PyPI:

        .. code-block:: sh

          pip install pyspark==3.2.0

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
          val sess = Glow.register(spark)
          val df = sess.read.format("vcf").load(path)

Notebooks embedded in the docs
------------------------------

Documentation pages are accompanied by embedded notebook examples. Most code in these notebooks can be run on Spark and Glow alone, but functions such as ``display()`` or ``dbutils()`` are only available on Databricks. See :ref:`dbnotebooks` for more info.

These notebooks are located in the Glow github repository `here <https://github.com/projectglow/glow/blob/master/docs/source/_static/zzz_GENERATED_NOTEBOOK_SOURCE/>`_ and are tested nightly end-to-end.  They include notebooks to define constants such as the number of samples to simulate and the output paths for each step in the pipeline. Notebooks that define constants are ``%run`` at the start of each notebook in the documentation. Please see :ref:`data_simulation` to get started.

Getting started on Databricks
-----------------------------

The Databricks documentation shows how to get started with Glow on, 

  - **Amazon Web Services** (AWS - `docs <https://docs.databricks.com/applications/genomics/tertiary-analytics/glow.html>`_)
  - **Microsoft Azure** (`docs <https://docs.microsoft.com/en-us/azure/databricks/applications/genomics/tertiary-analytics/glow>`_) 
  - **Google Cloud Platform** (GCP - `docs <https://docs.gcp.databricks.com/applications/genomics/tertiary-analytics/glow.html>`_)

 
Getting started on other cloud services
---------------------------------------

Please submit a pull request to add a guide for other cloud services.
