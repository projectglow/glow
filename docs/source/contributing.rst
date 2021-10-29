.. _contributing:

============
Contributing
============

Glow started as an industry collaboration between Databricks and the Regeneron Genetics Center.
But at its core it is a community driven project for scientists and engineers to work more effectively with genomics data.
We welcome contributions at all levels, and will work with you to include them in the project.

The sections below detail how to contribute.

------------
Raise Issues
------------

If you get stuck or hit errors when using Glow, please raise an `issue <https://github.com/projectglow/glow/issues>`_. 
Even if you solve the issue yourself, there's a good chance someone else will also encounter it. 

Please raise issues when you encounter them!

-----------------------------
Join the monthly office hours
-----------------------------

Once you have started using Glow, join the monthly office hours to keep up to date with new developments.
Please send an email to glowmaintainers [at] gmail.com to be added.


--------------------------
Contribute to the codebase
--------------------------

To contribute to Glow, please fork the library and create a branch.
Then you can create a pull request to contribute back to Glow.

.. important::
   
   Please sign off all commits! 

   .. code-block:: bash

      git commit -m "initial commit" --signoff 


.. _modify-add-notebooks:

1. Modify or add notebooks
==========================

.. _improve-documentation:

2. Improve the documentation
==========================

If you do add a notebook, please add it to a relevant or new section of the documentation.

.. _docker-environment:

3. Add libraries to the glow docker environment
===============================================

Please add any libraries you use to the glow docker container.
These libraries could be directly upstream or downstream of Glow, or used with the Glow :ref:`Pipe Transformer <pipe-transformer>`.

Please edit the `genomics Dockerfile <https://github.com/projectglow/glow/blob/master/docker/databricks/dbr/dbr9.1/genomics/Dockerfile>`_, which contains command line tools, Python and R packages._
Then we will build and test the container.

.. _features-bug-fixes:

4. Contribute new features / bug fixes
======================================






