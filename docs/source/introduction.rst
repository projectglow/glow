Introduction to Glow
====================

Glow aims to simplify genomic workflows at scale. The best way to accomplish this goal is to take a system that
has already been proven to work and adapt it to fit into the genomics ecosystem.

Apache Spark and in particular `Spark SQL <https://spark.apache.org/sql/>`_, its module for working with
structured data, is used at organizations across industries with datasets at the petabyte scale and
beyond. Glow smoothes the rough edges so that you can be productive immediately.

Glow features:

- Genomic datasources: To read datasets in common file formats like VCF and BGEN into Spark SQL DataFrames.
- Genomic functions: Common operations like computing quality control statistics, running regression
  tests, and performing simple transformations are provided as Spark SQL functions that can be
  called from Python, SQL, Scala, or R.
- Data preparation building blocks: Glow includes transformations like variant normalization and
  lift over to help produce analysis ready datasets.
- Integration with existing tools: With Spark SQL, you can write user-defined functions (UDFs) in
  Python, R, or Scala. Glow also makes it easy to run DataFrames through command line tools.
- Integration with other data types: Genomic data can generate additional insights when joined with data sets
  such as electronic health records, real world evidence, and medical images. Since Glow returns native Spark
  SQL DataFrames, its simple to join multiple data sets together.
