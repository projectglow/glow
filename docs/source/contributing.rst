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

-----------------------------
Join the monthly office hours
-----------------------------

Monthly office hours provide an opportunity keep up to date with new developments with Glow.
Please send an email to glow [dot] contributors [at] gmail.com to join.

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

As you work through the example notebooks in the docs, please document issues.
If you solve problems or improve code, please help contribute changes back.
That way others will benefit and become more productive.

Export your notebook as `html` into the relevant directory under `docs/source/_static/notebooks`.

And run this python script (swapping the html file out for your own).

.. code-block:: bash::
   
   python3 docs/dev/gen-nb-src.py --html docs/source/_static/notebooks/tertiary/pipe-transformer-vep.html

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

.. code-block:: bash 

   conda env create -f source/environment.yml

activate the glow docs conda environment:

.. code-block:: bash 

   conda activate glow-docs

build the docs:

.. code-block:: bash 

   make livehtml

connect to the local server via your browser at: `http://127.0.0.1:8000`


.. _docker-environment:

3. Add libraries to the glow docker environment
===============================================

Please edit glow `docker files <https://github.com/projectglow/glow/blob/master/docker/README.md>`_ to add libraries that integrate with glow.
Only include libraries that are used directly upstream or downstream of glow, or used with the glow :ref:`pipe transformer <pipe-transformer>`.

1. Setup a dockerhub account
2. Edit the `genomics docker file <https://github.com/projectglow/glow/blob/master/docker/databricks/dbr/dbr9.1/genomics/Dockerfile>`_ on your fork 

  - This file contains command line tools, Python and R packages

3. Build and push the container

  - Use this `bash script <https://github.com/projectglow/glow/blob/master/docker/databricks/build.sh>`_ as a template

4. Test the container in your environment in a notebook
5. Once you are happy with the container and the test, open a pull request

  - We will build and push the container to the official projectglow `dockerhub <https://hub.docker.com/u/projectglow>`_
  - Point to this container in the glow nightly continuous integration test `jobs definition <https://github.com/projectglow/glow/tree/master/docs/dev>`_
  - Once the circle-ci continuous integration test passes, we will incorporate it into the project

.. _features-bug-fixes:

4. Contribute new features / bug fixes
======================================

Here are example pull requests for new features or bug fixes that touch different aspects of the codebase,

- `Scala <https://github.com/projectglow/glow/pull/418>`_
- `Python functions <https://github.com/projectglow/glow/pull/416>`_
- `Python & R notebooks <https://github.com/projectglow/glow/pull/431>`_
- `Data schemas <https://github.com/projectglow/glow/pull/402>`_
- `Docker <https://github.com/projectglow/glow/pull/420>`_
- `Benchmarks <https://github.com/projectglow/glow/pull/440>`_

Much of the codebase is in Scala, however we are increasingly moving to Python.
Near-term focus is around integrating with Delta streaming and sharing.
In the future we will optimize code in C++.
