Introduction to Glow
====================

Glow aims to simplify genomic workflows at scale. The best way to accomplish this goal is to take a system that
has already been proven to work and adapt it to fit into the genomics ecosystem.

Apache Spark and in particular `Spark SQL <https://spark.apache.org/sql/>`_, its module for working with
structured data, is used at organizations across industries with datasets at the petabyte scale and
beyond. Glow seamlessly integrates common bioinformatics tools into this big data ecosystem so that 
you can be productive immediately.

Glow features:

- Genomic datasources: To read datasets in common file formats like VCF, BGEN and Plink into Spark SQL DataFrames.
- Genomic functions: Common operations such as computing quality control statistics, running regression
  tests, and performing simple transformations are provided as Spark SQL functions that can be
  called from Python, SQL, Scala, or R.
- Data preparation building blocks: Glow includes transformations such as variant normalization and
  liftOver to help produce analysis ready datasets.
- Integration with existing code and tools: With Spark SQL, you can write user-defined functions (UDFs) to 
  integrate Python, R, or Scala code into Spark. Glow also makes it easy to run DataFrames through command line 
  bioinformatics tools.
- Integration with other data types: Genomic data can generate additional insights when joined with data sets
  such as electronic health records, real world evidence, and medical images. Since Glow returns native Spark
  SQL DataFrames, its simple to join disparate data sources together.
