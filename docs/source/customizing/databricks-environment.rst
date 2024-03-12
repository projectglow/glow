.. meta::
  :description: Customizing the Databricks environment

======================================
Customizing the Databricks environment
======================================

Glow users often want to include additional resources inside the Databricks node environment.
For instance, :ref:`variant normalization <variantnormalization>` requires a reference genome,
:ref:`variant liftover <liftover>` requires a chain file, and the :ref:`pipe transformer <pipe-transformer>`
can be used to integrate with command line tools. You can ensure that these resources
are available on every node in a cluster by using `Databricks Container Services <https://docs.databricks.com/en/compute/custom-containers.html>`_
or `init scripts <https://docs.databricks.com/en/init-scripts/index.html>`_.

Init scripts
------------

`Init scripts <https://docs.databricks.com/en/init-scripts/index.html>`_ are useful for downloading small resources to a cluster. For example, the following script downloads
a liftover chain file from DBFS:

.. code-block:: sh

    mkdir -p /databricks/chain-files
    cp /dbfs/mnt/genomics/my-chain-file.chain  /databricks/chain-files

The script is guaranteed to run on every node in a cluster. You can then rely on the chain file for :ref:`variant liftover <liftover>`.


Databricks Container Services
-----------------------------

To avoid spending time running setup commands on each node in a cluster, we recommend packaging more complex dependencies with `Databricks Container Services <https://docs.databricks.com/en/compute/custom-containers.html>`_.

For example, the following :download:`Dockerfile <../_static/docker/Dockerfile>` based on DBR 14.3 LTS includes Glow, various bioinformatics tools, and a liftover chain file. You can modify this file to install whatever resources
you require.

.. literalinclude:: ../_static/docker/Dockerfile
   :language: sh
