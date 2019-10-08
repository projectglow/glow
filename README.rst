==================================
Glow: Genomics on Apache Spark SQL
==================================

Glow is a library to ETL genomics data using Apache Spark SQL.

|circle-ci|

.. |circle-ci| image:: https://circleci.com/gh/projectglow/glow.svg?style=svg&circle-token=7511f70b2c810a18e88b5c537b0410e82db8617d
    :target: https://circleci.com/gh/projectglow/glow

Building and Testing
--------------------
This project is built using sbt_.

.. _sbt: https://www.scala-sbt.org/1.0/docs/Setup.html

Start an sbt shell using the ``sbt`` command.

To compile the main code:

.. code-block:: sh

    compile


To run all tests:

.. code-block:: sh

    test

To test a specific suite:

.. code-block:: sh

    testOnly *VCFDataSourceSuite

If you use IntelliJ, you'll want to set up `scalafmt on save`_.

.. _scalafmt on save: https://scalameta.org/scalafmt/docs/installation.html

To ``test`` or ``testOnly`` in remote debug mode with IntelliJ IDEA set the remote debug configuration in IntelliJ to
'Attach to remote JVM' mode and a specific port number (here the default port number 5005 is used) and then modify the
definition of ``options`` in ``groupByHash`` function in ``build.sbt`` to:

.. code-block:: scala

    val options = ForkOptions().withRunJVMOptions(Vector("-Xmx1024m"))
        .withRunJVMOptions(Vector("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"))


To run Python tests, you must install and activate the conda environment in
``python/environment.yml``. You can then run tests from ``sbt``:

.. code-block:: sh

    python/test

These tests will run with the same Spark classpath as the Scala tests.
