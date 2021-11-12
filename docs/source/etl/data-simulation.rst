.. _data_simulation:

===============
Data Simulation
===============

The data simulation notebooks below generate phenotypes, covariates and genotypes at a user-defined scale.
This dataset can be used for integration and scale-testing.

.. _covariates_phenotypes:

Simulate Covariates & Phenotypes
================================

This data simulation notebooks uses Pandas to simulate quantitative and binary phenotypes and covariates.
Please ensure **n_samples** is the same as the genotype simulation notebook above.

.. notebook:: .. etl/1_simulate_covariates_phenotypes_offset.html

.. _genotypes:

Simulate Genotypes
==================

This data simulation notebook downloads chromosomes **21** and **22** from the 1000 Genomes Project,
and returns a Delta Lake table with a simulated set of genotypes for **n_samples** and **n_variants**,
maintaining hardy-weinberg equilibrium and allele frequency for each variant.

.. notebook:: .. etl/2_simulate_delta_pvcf.html
