.. _gwas_tutorial:

GWAS Tutorial
=============

This quickstart tutorial shows how to perform genome-wide association studies using Glow. 

Glow implements a distributed version of the `Regenie <https://rgcgithub.github.io/regenie/>`_ method.
Regenie's domain of applicability falls in analyzing data with extreme case/control imbalances, rare variants and/or diverse populations. 
Therefore it is suited for working with population-scale biobank exome or genome sequencing data.

.. tip::
   
   Other bioinformatics libraries for GWAS can be distributed using the :ref:`Glow Pipe Transformer <pipe-transformer>`.

You can view html versions of the notebooks and download them from the bottom of this page.

The notebooks are written in Python, with some visualization in R.


.. tip::
  
  We recommend running the :ref:`Data Simulation <data_simulation>` notebooks first to prepare data for this tutorial before trying with your own data.

.. important::

  Please sort phenotypes and covariates by sample ID in the same order as genotypes.

1. Quality Control
------------------

The first notebook in this series prepares data by performing standard quality control procedures on simulated genotype data.

2. Glow Whole Genome Regression (GloWGR)
----------------------------------------

:ref:`GloWGR <glowgr>` implements a distributed version of the Regenie method. 
Please review the Regenie paper in `Nature Genetics <https://doi.org/10.1038/s41588-021-00870-7>`_
and the `Regenie Github <https://github.com/rgcgithub/regenie>`_ repo before implementing this method on real data.

3. Regression
-------------

The GloWGR notebook calculated offsets that are used in the genetic association study below to control for population structure and relatedness.

.. notebook:: . tertiary/1_quality_control.html
  :title: Quality control

.. notebook:: . tertiary/2_quantitative_glowgr.html
  :title: Quantitative glow whole genome regression

.. notebook:: . tertiary/3_linear_gwas_glow.html
  :title: Linear regression

.. notebook:: . tertiary/4_binary_glowgr.html
  :title: Binary glow whole genome regression

.. notebook:: . tertiary/5_logistic_gwas_glow.html
  :title: Logistic regression



