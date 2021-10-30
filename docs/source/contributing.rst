.. _contributing:

============
Contributing
============

Glow started as an industry collaboration between databricks and the Regeneron Genetics Center.
The goal is to help scientists and engineers solve genomics problems with data.

We welcome contributions, and will work with you to include them in the project.

The sections below detail how to contribute.

------------
Raise Issues
------------

If you get stuck or hit errors when using glow, please raise an `issue <https://github.com/projectglow/glow/issues>`_. 
Even if you solve the problem, there's a good chance someone else will encounter it. 

.. important::
   
   Please raise issues!

-----------------------------
Join the monthly office hours
-----------------------------

Once you are using glow, the monthly office hours provide an opportunity keep up to date with new developments.
Please send an email to glow [dot] maintainers [at] gmail.com to join.

--------------------------
Contribute to the codebase
--------------------------

To contribute to glow, please fork the library and create a branch.
Make your changes and create a pull request.
It's easy to get started.

.. important::
   
   Please sign off all commits! 

   .. code-block:: bash

      git commit -m "initial commit" --signoff 


.. _modify-add-notebooks:

1. Modify or add notebooks
==========================

Modifying a notebook helps others understand the code.
As you work through a notebook, please document problems.

Export your notebook as `html`.

And run this python script

.. code-block:: bash::
   
   python3 docs/dev/gen-nb-src.py --html docs/source/_static/notebooks/tertiary/pipe-transformer-vep.html

changing the notebook path.

.. tip::

   notebooks run in linear order off of :ref:`simulated data <data_simulation>`

.. _improve-documentation:

2. Improve the documentation
==========================

If you add a notebook, please reference it in the documentation. 
Either to an existing docs page, or create a new one.
We welcome contributions that include, 

- tips for glow,
   - Spark cluster configuration and tuning
   - glow use cases
- troubleshooting guides and gotchas
- fix typos, hyperlinks or paths
- better explanations of,
   - what code snippets in the docs mean
   - what cells in notebooks mean
- unit tests for notebook code

To build the docs locally, 

first create the conda environment:

.. code-block:: bash 

   conda env create -f source/environment.yml

activate the glow docs conda environment:

.. code-block:: bash 

   conda activate glow-docs

build the docs:

.. code-block:: bash 

   make livehtml

connect to the local server via your browser at: `http://127.0.0.1:8000 <http://127.0.0.1:8000>`_


.. _docker-environment:

3. Add libraries to the glow docker environment
===============================================

Please edit glow `docker files <https://github.com/projectglow/glow/blob/master/docker/README.md>`_ to add libraries that integrate with glow.
Only include libraries that are used directly upstream or downstream of glow, or used with the glow :ref:`pipe transformer <pipe-transformer>`.

Please edit the `genomics docker file <https://github.com/projectglow/glow/blob/master/docker/databricks/dbr/dbr9.1/genomics/Dockerfile>`_, which contains command line tools, Python and R packages.
Then we will build and test the container.

.. _features-bug-fixes:

4. Contribute new features / bug fixes
======================================

A lot of the codebase is in Scala, however we are increasingly moving to Python.
Glow includes,

- Docker environments
- Schemas
- R vizualisations
- SQL benchmarks 

Near-term focus is around integrating with Delta streaming and sharing.
In the future we will optimize code in C++.

5. Example Contributions
========================

Here are example pull requests from the past few months,

- `Scala <>`
- `Python <>`
- `Data schemas <>`
- `Docker <>`
- `R <>`
- SQL Benchmarks
- Delta Lake integration





