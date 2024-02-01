:orphan:

.. _community-edition:

How to Use Glow on Databricks Community Edition
===============================================

Try Glow on Databricks for free with
`Databricks Community Edition <https://databricks.com/product/faq/community-edition>`_.

Step 1: Sign up for Community Edition
-------------------------------------

`Sign up <https://docs.databricks.com/getting-started/try-databricks.html>`_ for a free Databricks trial with
Databricks Community Edition.

1. Fill out the Databricks `free trial form <https://databricks.com/try-databricks>`_ and click "Sign Up".
2. In the "Community Edition" section, click "Get Started".
3. Click the "Get Started" link in your “Welcome to Databricks” verification email.
4. Set your password and click "Reset password". You will be redirected to your Databricks Community Edition workspace.

|div-clear|

Step 2: Import Glow notebooks
-----------------------------

`Import <https://docs.databricks.com/en/notebooks/notebook-export-import.html>`_ the Glow demonstration
notebooks to your Databricks Community Edition workspace.

.. image:: import-dbc.gif
   :alt: Import demo notebooks
   :align: right
   :scale: 32 %

1. Log into your `Databricks Community Edition workspace <https://community.cloud.databricks.com/login.html>`_.
2. Download the desired Glow notebooks, such as :download:`the GloWGR demo <../../_static/notebooks/tertiary/glowgr.html>`.
3. Click the `Workspace button <https://docs.databricks.com/workspace/workspace-objects.html#workspace-root-folder>`_
   in the left sidebar of your workspace.
4. In your user folder, right-click and select "Import".
5. Select "Import from file", select the downloaded notebook, and click "Import".

|div-clear|

Step 3: Create a cluster
------------------------

`Create <https://docs.databricks.com/clusters/create.html>`_ the cluster shortly before you run the notebooks; the
cluster will be automatically terminated after an idle period of 2 hours.

1. Log into your `Databricks Community Edition workspace <https://community.cloud.databricks.com/login.html>`_.
2. Click the `Clusters button <https://docs.databricks.com/clusters/create.html>`_ in the left sidebar of your
   workspace.
3. Set the "Cluster Name" as desired.
4. Click "Create Cluster".
5. Refresh the page to see your new cluster in the list.

|div-clear|

Step 4: Attach cluster-scoped libraries
---------------------------------------

`Install <https://docs.databricks.com/libraries/cluster-libraries.html>`_ libraries to a cluster in order to run
third-party code.

.. image:: attach-mlflow.gif
   :alt: Attach MLflow to the cluster
   :align: right
   :scale: 32 %

1. Log into your `Databricks Community Edition workspace <https://community.cloud.databricks.com/login.html>`_.
2. Click the `Clusters button <https://docs.databricks.com/clusters/create.html>`_ in the left sidebar of your
   workspace.
3. Click an existing cluster.
4. Select the "Libraries" tab.
5. Click "Install New."
6. Set the Library Source to ``maven`` and the Coordinates to ``io.projectglow:glow-spark3_2.12:${version}``. Click "Install".
7. Set the Library Source to ``PyPi`` and the Package to ``glow.py``. Click "Install".

|div-clear|

.. |div-clear| raw:: html

    <div style="clear: both"></div>
