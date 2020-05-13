=================
Utility Functions
=================

.. invisible-code-block: python

    import glow
    glow.register(spark)

Glow includes a variety of utility functions for performing basic data manipulation.

Struct transformations
======================

Glow's struct transformation functions change the schema structure of the DataFrame. These transformations integrate with functions
whose parameter structs require a certain schema.

- ``subset_struct``: subset fields from a struct

.. code-block:: python

    from pyspark.sql import Row
    row_one = Row(Row(str_col='foo', int_col=1, bool_col=True))
    row_two = Row(Row(str_col='bar', int_col=2, bool_col=False))
    base_df = spark.createDataFrame([row_one, row_two], schema=['base_col'])
    subsetted_df = base_df.selectExpr("subset_struct(base_col, 'str_col', 'bool_col') as subsetted_col")

.. invisible-code-block: python

   assert_rows_equal(subsetted_df.head().subsetted_col, Row(str_col='foo', bool_col=True))


- ``add_struct_fields``: append fields to a struct

.. code-block:: python

    added_df = base_df.selectExpr("add_struct_fields(base_col, 'float_col', 3.14, 'rev_str_col', reverse(base_col.str_col)) as added_col")

.. invisible-code-block: python

   from decimal import Decimal
   expected_added_col = Row(bool_col=True, int_col=1, str_col='foo', float_col=Decimal('3.14'), rev_str_col='oof')
   assert_rows_equal(added_df.head().added_col, expected_added_col)


- ``expand_struct``: explode a struct into columns

.. code-block:: python

    expanded_df = base_df.selectExpr("expand_struct(base_col)")

.. invisible-code-block: python

   assert_rows_equal(expanded_df.head(), Row(bool_col=True, int_col=1, str_col='foo'))


Spark ML transformations
========================

Glow supports transformations between double arrays and Spark ML vectors for integration with machine learning
libraries such as Spark's machine learning library (MLlib).

- ``array_to_dense_vector``: transform from an array to a dense vector

.. code-block:: python

    array_df = spark.createDataFrame([Row([1.0, 2.0, 3.0]), Row([4.1, 5.1, 6.1])], schema=['array_col'])
    dense_df = array_df.selectExpr('array_to_dense_vector(array_col) as dense_vector_col')

.. invisible-code-block: python

   from pyspark.ml.linalg import DenseVector
   assert dense_df.head().dense_vector_col == DenseVector([1.0, 2.0, 3.0])

- ``array_to_sparse_vector``: transform from an array to a sparse vector

.. code-block:: python

   sparse_df = array_df.selectExpr('array_to_sparse_vector(array_col) as sparse_vector_col')

.. invisible-code-block: python

   from pyspark.ml.linalg import SparseVector
   assert sparse_df.head().sparse_vector_col == SparseVector(3, {0: 1.0, 1: 2.0, 2: 3.0})

- ``vector_to_array``: transform from a vector to a double array

.. code-block:: python

    from pyspark.ml.linalg import SparseVector
    row_one = Row(vector_col=SparseVector(3, [0, 2], [1.0, 3.0]))
    row_two = Row(vector_col=SparseVector(3, [1], [1.0]))
    vector_df = spark.createDataFrame([row_one, row_two])
    array_df = vector_df.selectExpr('vector_to_array(vector_col) as array_col')

.. invisible-code-block: python

   assert array_df.head().array_col == [1.0, 0.0, 3.0]


- ``explode_matrix``: explode a Spark ML matrix such that each row becomes an array of doubles

.. code-block:: python

    from pyspark.ml.linalg import DenseMatrix
    matrix_df = spark.createDataFrame(Row([DenseMatrix(2, 3, range(6))]), schema=['matrix_col'])
    array_df = matrix_df.selectExpr('explode_matrix(matrix_col) as array_col')

.. invisible-code-block: python

   assert array_df.head().array_col == [0.0, 2.0, 4.0]

.. _variant-data-transformations:

Variant data transformations
============================

Glow supports numeric transformations on variant data for downstream calculations, such as GWAS.

- ``genotype_states``: create a numeric representation for each sample's genotype data. This calculates the sum of the
  calls (or ``-1`` if any calls are missing); the sum is equivalent to the number of alternate alleles for biallelic
  variants.

.. code-block:: python

    from pyspark.sql.types import *

    missing_and_hom_ref = Row([Row(calls=[-1,0]), Row(calls=[0,0])])
    het_and_hom_alt = Row([Row(calls=[0,1]), Row(calls=[1,1])])
    calls_schema = StructField('calls', ArrayType(IntegerType()))
    genotypes_schema = StructField('genotypes_col', ArrayType(StructType([calls_schema])))
    genotypes_df = spark.createDataFrame([missing_and_hom_ref, het_and_hom_alt], StructType([genotypes_schema]))
    num_alt_alleles_df = genotypes_df.selectExpr('genotype_states(genotypes_col) as num_alt_alleles_col')

.. invisible-code-block: python

   assert num_alt_alleles_df.head().num_alt_alleles_col == [-1, 0]

- ``hard_calls``: get hard calls from genotype probabilities. These are determined based on the number of alternate
  alleles for the variant, whether the probabilities are phased (true for haplotypes and false for genotypes), and a
  call threshold (if not provided, this defaults to ``0.9``). If no calls have a probability above the threshold, the
  call is set to ``-1``.

.. code-block:: python

    unphased_above_threshold = Row(probabilities=[0.0, 0.0, 0.0, 1.0, 0.0, 0.0], num_alts=2, phased=False)
    phased_below_threshold = Row(probabilities=[0.1, 0.9, 0.8, 0.2], num_alts=1, phased=True)
    uncalled_df = spark.createDataFrame([unphased_above_threshold, phased_below_threshold])
    hard_calls_df = uncalled_df.selectExpr('hard_calls(probabilities, num_alts, phased, 0.95) as calls')

.. invisible-code-block: python

   assert hard_calls_df.head().calls == [2, 0]

- ``mean_substitute``: substitutes the missing values of a numeric array using the mean of the non-missing values. Any
  values that are NaN, null or equal to the missing value parameter are considered missing. If all values are missing,
  they are substituted with the missing value. If the missing value is not provided, this defaults to ``-1``.

.. code-block:: python

    unsubstituted_row = Row(unsubstituted_values=[float('nan'), None, -1.0, 0.0, 1.0, 2.0, 3.0])
    unsubstituted_df = spark.createDataFrame([unsubstituted_row])
    substituted_df = unsubstituted_df.selectExpr("mean_substitute(unsubstituted_values, -1.0) as substituted_values")

.. invisible-code-block: python

   assert substituted_df.head().substituted_values == [1.5, 1.5, 1.5, 0, 1, 2, 3]
