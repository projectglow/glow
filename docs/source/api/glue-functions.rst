==============
Glue Functions
==============

To allow for interoperability between different bioinformatics tools, Glow includes a variety of glue functions.

Struct transformations
======================

Subset or append fields based on an initial struct with the ``subset_struct`` and ``add_struct_fields`` functions
respectively. Use the ``expand_struct`` function to explode a struct into columns.

.. code-block:: py

    from pyspark.sql import Row
    row_one = Row(Row(str_col='foo', int_col=1, bool_col=True))
    row_two = Row(Row(str_col='bar', int_col=2, bool_col=False))
    base_df = spark.createDataFrame([row_one, row_two], schema=['base_col'])
    subset_df = base_df.selectExpr("subset_struct(base_col, 'str_col', 'bool_col') as subsetted_col")
    added_df = subset_df.selectExpr("add_struct_fields(subsetted_col, 'float_col', 3.14, 'rev_str_col', \
        reverse(subsetted_col.str_col)) as added_col")
    expanded_df = added_df.selectExpr("expand_struct(added_col)")


Spark ML transformations
========================

Glow supports transformations between double arrays and Spark ML vectors. To transform from an array to a dense
vector, use ``array_to_dense_vector``; from an array to a sparse vector, use ``array_to_sparse_vector``. To transform
a vector to a double array, use ``vector_to_array``.

.. code-block:: py

    from pyspark.sql import Row
    array_df = spark.createDataFrame([Row([1.0, 2.0, 3.0]), Row([4.1, 5.1, 6.1])], schema=['array_col'])
    dense_vector_df = array_df.selectExpr('array_to_dense_vector(array_col) as dense_vector_col')
    sparse_vector_df = array_df.selectExpr('array_to_sparse_vector(array_col) as sparse_vector_col')
    roundtrip_array_df = sparse_vector_df.selectExpr('vector_to_array(sparse_vector_col) as array_col')

Glow also supports exploding a Spark ML matrix with ``explode_matrix`` such that each row becomes an array of
doubles.

.. code-block:: py

    from pyspark.ml.linalg import DenseMatrix
    from pyspark.sql import Row
    matrix_df = spark.createDataFrame(Row([DenseMatrix(2, 3, range(6))]), schema=['matrix_col'])
    array_df = matrix_df.selectExpr('explode_matrix(matrix_col) as array_col')

Variant data transformations
============================

Glow supports transformations specific to variant data.

Create a numeric representation for each sample's genotype data with the ``genotype_states`` function, which calculates
the sum of the calls (or ``-1`` if any calls are missing); this is equivalent to the number of alternate alleles for
biallelic variants.

.. code-block:: py

    from pyspark.sql import Row
    from pyspark.sql.types import *

    missing_and_hom_ref = Row([Row(calls=[-1,0]), Row(calls=[0,0])])
    het_and_hom_alt = Row([Row(calls=[0,1]), Row(calls=[1,1])])
    calls_schema = StructField('calls', ArrayType(IntegerType()))
    genotypes_schema = StructField('genotypes_col', ArrayType(StructType([calls_schema])))
    genotypes_df = spark.createDataFrame([missing_and_hom_ref, het_and_hom_alt], StructType([genotypes_schema]))
    num_alt_alleles_df = genotypes_df.selectExpr('genotype_states(genotypes_col) as num_alt_alleles_col')

Get hard calls from genotype probabilities using the ``hard_calls`` function; these are determined based on the number
of alternate alleles for the variant, whether the probabilities are phased (true for haplotypes and false for
genotypes), and a call threshold (if not provided, this defaults to ``0.9``). If no calls have a probability above the
threshold, the call is set to ``-1``.

.. code-block:: py

    from pyspark.sql import Row
    unphased_above_threshold = Row(probabilities=[0.0, 0.0, 0.0, 1.0, 0.0, 0.0], num_alts=2, phased=False)
    phased_below_threshold = Row(probabilities=[0.1, 0.9, 0.8, 0.2], num_alts=1, phased=True)
    uncalled_df = spark.createDataFrame([unphased_above_threshold, phased_below_threshold])
    hard_calls_df = uncalled_df.selectExpr('hard_calls(probabilities, num_alts, phased, 0.95) as calls')
