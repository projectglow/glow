.. _data_simulation:

===============
Data Simulation
===============

These data simulation notebooks generate phenotypes, covariates and genotypes at a user-defined scale.
This dataset can be used for integration and scale-testing.

The variables **n_samples** and **n_variants** for defining this scale are in the notebook ``0_setup_constants_glow``. This notebook is ``%run`` from the notebooks below using its relative path. The notebook is located in the Glow github repository `here <https://github.com/projectglow/glow/blob/master/docs/source/_static/zzz_GENERATED_NOTEBOOK_SOURCE/0_setup_constants_glow.py>`_.

.. _covariates_phenotypes:

Simulate Covariates & Phenotypes
================================

This data simulation notebooks uses Pandas to simulate quantitative and binary phenotypes and covariates.

.. notebook:: .. etl/1_simulate_covariates_phenotypes_offset.html

.. _genotypes:

Simulate Genotypes
==================

This data simulation notebook loads variant call format (VCF) files from the 1000 Genomes Project,
and returns a Delta Lake table with simulated genotypes, maintaining hardy-weinberg equilibrium and allele frequency for each variant.

.. notebook:: .. etl/2_simulate_delta_pvcf.html
