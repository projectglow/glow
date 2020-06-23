.. _vcf2delta:

============================
Create a Genomics Delta Lake
============================

Genomics data is usually stored in specialized flat-file formats such as VCF or BGEN.

The example below shows how to ingest a VCF into a genomics `Delta Lake table <https://delta.io>`_ using Glow in Python
(R, Scala, and SQL are also supported).

You can use Delta tables for second-latency queries, performant range-joins (similar to the single-node
bioinformatics tool bedtools intersect), aggregate analyses such as calculating summary statistics,
machine learning or deep learning.

.. tip:: We recommend ingesting VCF files into Delta tables once volumes reach >1000 samples, >10 billion genotypes or >1 terabyte.

.. notebook:: .. etl/vcf2delta.html
  :title: VCF to Delta Lake table notebook
