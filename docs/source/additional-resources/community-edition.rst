:orphan:

.. _community-edition:

Use Databricks Community Edition
================================

Try Glow for free with `Databricks Community Edition <https://databricks.com/product/faq/community-edition>`_.

Step 1: Sign up for Community Edition
-------------------------------------

1. Fill out the Databricks `free trial form <databricks.com/try>`_.
2. Select Community Edition.
3. Fill in the registration form.
4. Click Sign Up.
5. Read the Terms of Service and click Agree.
6. When you receive the “Welcome to Databricks” email, click the link to verify your mail address.
7. Log into Databricks using the supplied credentials.

Step 2: Enable the Databricks Runtime for Genomics
--------------------------------------------------

1. Navigate to your `Community Edition workspace <https://community.cloud.databricks.com/>`_.
2. Navigate to the `admin console <https://docs.databricks.com/administration-guide/admin-console.html>`_.
3. Select the Advanced tab.
4. Enable the Databricks Runtime for Genomics.
5. Refresh the page for this setting to take effect.

Step 3: Import Glow notebooks
-----------------------------

1. Click the Workspace button in your `Community Edition workspace <https://community.cloud.databricks.com/>`_.
2. In your user folder, right-click and select Import.
3. Click Import from URL.
4. Set the URL to :download:`this DBC archive <../_static/dbc/glow-demo.dbc>`.
5. Click Import.

Step 4: Create a cluster
------------------------

Create the cluster shortly before you run the notebooks; the cluster will automatically terminated after an idle period
of 2 hours. The cluster may take up to 10 minutes to set up.

1. Set the Runtime Version to the latest version of the Genomics Runtime, which includes Glow.
2. Under the Spark tab, set the Environment Variable `refGenomeId=grch38`; this will initialize the cluster with human
   genome assembly 38 installed.
3. Click Create Cluster. As the cluster is being created, attach the following
  `PyPi libraries <https://docs.databricks.com/libraries.html#pypi-package>`_: `bioinfokit` and `mlflow`.
