.. _vcf2delta:

============================
Create a Genomics Delta Lake
============================

Genomics data is stored in specialized flat-file formats such as VCF or BGEN.
However, building robust data engineering pipelines requires the use of database technology that scales to the expected data volume.
And for computational biology / bioinformatics use cases it also requires support for not only SQL, but also Python, R, and command-line bioinformatics tools.

The example below shows how to ingest a VCF into a genomics `Delta Lake table <https://delta.io>`_ using Glow.
Delta Lake supports Scala, Python, R and SQL. Bioinformatics tools can also be integrated into your data pipeline with the :ref:`Glow Pipe Transformer <pipe-transformer>`.

The example explodes a project-level VCF (pVCF) with many genotypes per row (represented as an array of structs),
into a form with one genotype and one variant per row. In this representation Delta Lake can be efficiently queried at the genotype or gene level.

Then we will register the Delta Lake as a Spark SQL table, perform point queries, and then gene-level queries using annotation data from the :ref:`gff <gff>` demo.

.. tip:: We recommend ingesting VCF files into Delta tables once volumes reach >1000 samples, >10 billion genotypes or >1 terabyte.

.. notebook:: .. etl/6_explode_variant_dataframe.html
  :title: Explode pVCF variant dataframe and write to Delta Lake

.. notebook:: .. etl/8_create_database_for_querying.html
  :title: Create database for variants and annotations

.. notebook:: .. etl/9_query_variant_db.html
  :title: Query variant database
