.. _contributing:

============
Contributing
============

Glow began as an industry collaboration between databricks and the Regeneron Genetics Center.
Glow enables scientists and engineers work together to solve genomics problems with data.

Contributing is easy, and we will collaborate with you to extend the project.

The sections below detail how to contribute.

------------
Raise Issues
------------

If you get stuck or hit errors when using glow, please raise an `issue <https://github.com/projectglow/glow/issues>`_. 
Even if you solve the problem, there's a good chance someone else will encounter it. 

.. important::
   
   Please raise issues!

--------------------------
Contribute to the codebase
--------------------------

To contribute to glow, please fork the library and create a branch.
Make your changes and create a pull request.
It's easy to get started.

.. important::
   
   Please sign off all commits! 

   .. code-block:: sh

      git commit -m "initial commit" --signoff 


.. _modify-add-notebooks:

1. Modify or add notebooks
==========================

As you work through the example notebooks in the docs, please document issues.
If you solve problems or improve code, please help contribute changes back.
That way others will benefit and become more productive.

Export your notebook as `html` into the relevant directory under `docs/source/_static/notebooks`.

And run this python script (swapping the html file out for your own).

.. code-block:: sh::
   
   python3 docs/dev/gen-nb-src.py --html docs/source/_static/notebooks/tertiary/pipe-transformer-vep.html

The Glow workflow is tested in a nightly integration test in Databricks.
If you add notebooks or rename them, please also edit the workflow definition json located in `docs/dev/ <https://github.com/projectglow/glow/blob/master/docs/dev>`_.

.. _improve-documentation:

2. Improve the documentation
============================

If you add a notebook, please reference it in the documentation. 
Either to an existing docs page, or create a new one.
Other contributions to the docs include, 

- Tips for glow

   - Spark cluster configuration and tuning
   - glow use cases

- Troubleshooting guides and gotchas
- Fix typos, hyperlinks or paths
- Better explanations of

   - what code snippets in the docs mean?
   - what cells in notebooks mean?

- Unit tests for notebook code
- New use cases

To build the docs locally, 

first create the conda environment:

.. code-block:: sh

   cd docs
   conda env create -f source/environment.yml

activate the glow docs conda environment:

.. code-block:: sh

   conda activate glow-docs

build the docs:

.. code-block:: sh

   make livehtml

connect to the local server via your browser at: `http://127.0.0.1:8000`


3. Contribute new features / bug fixes
======================================

Here are example pull requests for new features or bug fixes that touch different aspects of the codebase,

- `Scala <https://github.com/projectglow/glow/pull/418>`_
- `Python functions <https://github.com/projectglow/glow/pull/416>`_
- `Python & R notebooks <https://github.com/projectglow/glow/pull/431>`_
- `Data schemas <https://github.com/projectglow/glow/pull/402>`_
- `Benchmarks <https://github.com/projectglow/glow/pull/440>`_

Much of the codebase is in Scala, however we are increasingly moving to Python.
Near-term focus is around integrating with Delta streaming and sharing.
