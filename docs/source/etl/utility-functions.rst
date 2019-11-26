=================
Utility Functions
=================

.. testsetup::

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.config('spark.jars.packages', 'io.projectglow:glow_2.11:0.1.2').getOrCreate()

    import glow
    glow.register(spark)


Glow includes a variety of utility functions for performing basic data manipulation.

Struct transformations
======================

Glow's struct transformation functions change the schema of a struct. These transformations integrate with functions
whose parameter structs require a certain schema.

- ``subset_struct``: subset fields from a struct

.. doctest::

    >>> from pyspark.sql import Row
    >>> row_one = Row(Row(str_col='foo', int_col=1, bool_col=True))
    >>> row_two = Row(Row(str_col='bar', int_col=2, bool_col=False))
    >>> base_df = spark.createDataFrame([row_one, row_two], schema=['base_col'])
    >>> base_df.selectExpr("subset_struct(base_col, 'str_col', 'bool_col') as subsetted_col").show()
    +-------------+
    |subsetted_col|
    +-------------+
    |  [foo, true]|
    | [bar, false]|
    +-------------+
    <BLANKLINE>

- ``add_struct_fields``: append fields to a struct

.. code-block:: py

    base_df.selectExpr("add_struct_fields(base_col, 'float_col', 3.14, 'rev_str_col', reverse(base_col.str_col)) as added_col")

- ``expand_struct``: explode a struct into columns

.. code-block:: py

    base_df.selectExpr("expand_struct(base_col)")


Spark ML transformations
========================

Glow supports transformations between double arrays and Spark ML vectors for integration with machine learning
libraries such as MLlib.

- ``array_to_dense_vector``: transform from an array to a dense vector

.. code-block:: py

    array_df = spark.createDataFrame([Row([1.0, 2.0, 3.0]), Row([4.1, 5.1, 6.1])], schema=['array_col'])
    array_df.selectExpr('array_to_dense_vector(array_col) as dense_vector_col')

- ``array_to_sparse_vector``: transform from an array to a sparse vector

.. code-block:: py

    array_df.selectExpr('array_to_sparse_vector(array_col) as sparse_vector_col')

- ``vector_to_array``: transform from a vector to a double array

.. code-block:: py

    from pyspark.ml.linalg import SparseVector
    row_one = Row(vector_col=SparseVector(3, [0, 2], [1.0, 3.0]))
    row_two = Row(vector_col=SparseVector(3, [1], [1.0]))
    vector_df = spark.createDataFrame([row_one, row_two])
    vector_df.selectExpr('vector_to_array(vector_col) as array_col')

- ``explode_matrix``: explode a Spark ML matrix such that each row becomes an array of doubles

.. code-block:: py

    from pyspark.ml.linalg import DenseMatrix
    matrix_df = spark.createDataFrame(Row([DenseMatrix(2, 3, range(6))]), schema=['matrix_col'])
    matrix_df.selectExpr('explode_matrix(matrix_col) as array_col')

Variant data transformations
============================

Glow supports numeric transformations on variant data for downstream calculations, such as GWAS.

- ``genotype_states``: create a numeric representation for each sample's genotype data. This calculates the sum of the
  calls (or ``-1`` if any calls are missing); the sum is equivalent to the number of alternate alleles for biallelic
  variants.

.. code-block:: py

    from pyspark.sql.types import *

    missing_and_hom_ref = Row([Row(calls=[-1,0]), Row(calls=[0,0])])
    het_and_hom_alt = Row([Row(calls=[0,1]), Row(calls=[1,1])])
    calls_schema = StructField('calls', ArrayType(IntegerType()))
    genotypes_schema = StructField('genotypes_col', ArrayType(StructType([calls_schema])))
    genotypes_df = spark.createDataFrame([missing_and_hom_ref, het_and_hom_alt], StructType([genotypes_schema]))
    num_alt_alleles_df = genotypes_df.selectExpr('genotype_states(genotypes_col) as num_alt_alleles_col')

- ``hard_calls``: get hard calls from genotype probabilities. These are determined based on the number of alternate
  alleles for the variant, whether the probabilities are phased (true for haplotypes and false for genotypes), and a
  call threshold (if not provided, this defaults to ``0.9``). If no calls have a probability above the threshold, the
  call is set to ``-1``.

.. code-block:: py

    unphased_above_threshold = Row(probabilities=[0.0, 0.0, 0.0, 1.0, 0.0, 0.0], num_alts=2, phased=False)
    phased_below_threshold = Row(probabilities=[0.1, 0.9, 0.8, 0.2], num_alts=1, phased=True)
    uncalled_df = spark.createDataFrame([unphased_above_threshold, phased_below_threshold])
    hard_calls_df = uncalled_df.selectExpr('hard_calls(probabilities, num_alts, phased, 0.95) as calls')
