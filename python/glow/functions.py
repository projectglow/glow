# The Glow Python functions
# Note that this file is generated from the definitions in functions.yml.

from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column, _to_seq
from typeguard import check_argument_types, check_return_type
from typing import Union

def sc():
    return SparkContext._active_spark_context

########### complex_type_manipulation

def add_struct_fields(struct: Union[Column, str], *fields: Union[Column, str]) -> Column:
    """
    Add fields to a struct

    Added in version 0.3.0.

    Examples:
        >>> df = spark.createDataFrame([Row(struct=Row(a=1))])
        >>> df.select(glow.add_struct_fields('struct', lit('b'), lit(2)).alias('struct')).collect()
        [Row(struct=Row(a=1, b=2))]

    Args:
    struct : The struct to which fields will be added
    fields : New fields
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.add_struct_fields(_to_java_column(struct), _to_seq(sc(), fields, _to_java_column)))
    assert check_return_type(output)
    return output
  

def array_summary_stats(arr: Union[Column, str]) -> Column:
    """
    Compute the min, max, mean, stddev for an array of numerics

    Added in version 0.3.0.

    Examples:
        >>> df = spark.createDataFrame([Row(arr=[1, 2, 3])])
        >>> df.select(glow.expand_struct(glow.array_summary_stats('arr'))).collect()
        [Row(mean=2.0, stdDev=1.0, min=1.0, max=3.0)]

    Args:
    arr : The array of numerics
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.array_summary_stats(_to_java_column(arr)))
    assert check_return_type(output)
    return output
  

def array_to_dense_vector(arr: Union[Column, str]) -> Column:
    """
    Convert an array of numerics into a spark.ml DenseVector

    Added in version 0.3.0.

    Examples:
        >>> from pyspark.ml.linalg import DenseVector
        >>> df = spark.createDataFrame([Row(arr=[1, 2, 3])])
        >>> df.select(glow.array_to_dense_vector('arr').alias('v')).collect()
        [Row(v=DenseVector([1.0, 2.0, 3.0]))]

    Args:
    arr : The array of numerics
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.array_to_dense_vector(_to_java_column(arr)))
    assert check_return_type(output)
    return output
  

def array_to_sparse_vector(arr: Union[Column, str]) -> Column:
    """
    Convert an array of numerics into a spark.ml SparseVector

    Added in version 0.3.0.

    Examples:
        >>> from pyspark.ml.linalg import SparseVector
        >>> df = spark.createDataFrame([Row(arr=[1, 0, 2, 0, 3, 0])])
        >>> df.select(glow.array_to_sparse_vector('arr').alias('v')).collect()
        [Row(v=SparseVector(6, {0: 1.0, 2: 2.0, 4: 3.0}))]

    Args:
    arr : The array of numerics
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.array_to_sparse_vector(_to_java_column(arr)))
    assert check_return_type(output)
    return output
  

def expand_struct(struct: Union[Column, str]) -> Column:
    """
    Promote fields of a nested struct to top-level columns. Similar to using struct.* from SQL, but can be used in more contexts.

    Added in version 0.3.0.

    Examples:
        >>> df = spark.createDataFrame([Row(struct=Row(a=1, b=2))])
        >>> df.select(glow.expand_struct(col('struct'))).collect()
        [Row(a=1, b=2)]

    Args:
    struct : The struct to expand
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.expand_struct(_to_java_column(struct)))
    assert check_return_type(output)
    return output
  

def explode_matrix(matrix: Union[Column, str]) -> Column:
    """
    Explode a spark.ml Matrix into arrays of rows

    Added in version 0.3.0.

    Examples:
        >>> from pyspark.ml.linalg import DenseMatrix
        >>> m = DenseMatrix(numRows=3, numCols=2, values=[1, 2, 3, 4, 5, 6])
        >>> df = spark.createDataFrame([Row(matrix=m)])
        >>> df.select(glow.explode_matrix('matrix').alias('row')).collect()
        [Row(row=[1.0, 4.0]), Row(row=[2.0, 5.0]), Row(row=[3.0, 6.0])]

    Args:
    matrix : The matrix to explode
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.explode_matrix(_to_java_column(matrix)))
    assert check_return_type(output)
    return output
  

def subset_struct(struct: Union[Column, str], *fields: str) -> Column:
    """
    Select fields from a struct

    Added in version 0.3.0.

    Examples:
        >>> df = spark.createDataFrame([Row(struct=Row(a=1, b=2, c=3))])
        >>> df.select(glow.subset_struct('struct', 'a', 'c').alias('struct')).collect()
        [Row(struct=Row(a=1, c=3))]

    Args:
    struct : Struct from which to select fields
    fields : Fields to take
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.subset_struct(_to_java_column(struct), _to_seq(sc(), fields)))
    assert check_return_type(output)
    return output
  

def vector_to_array(vector: Union[Column, str]) -> Column:
    """
    Convert a spark.ml vector (sparse or dense) to an array of doubles

    Added in version 0.3.0.

    Examples:
        >>> from pyspark.ml.linalg import DenseVector, SparseVector
        >>> df = spark.createDataFrame([Row(v=SparseVector(3, {0: 1.0, 2: 2.0})), Row(v=DenseVector([3.0, 4.0]))])
        >>> df.select(glow.vector_to_array('v').alias('arr')).collect()
        [Row(arr=[1.0, 0.0, 2.0]), Row(arr=[3.0, 4.0])]

    Args:
    vector : Vector to convert
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.vector_to_array(_to_java_column(vector)))
    assert check_return_type(output)
    return output
  
########### etl

def hard_calls(probabilities: Union[Column, str], numAlts: Union[Column, str], phased: Union[Column, str], threshold: float = None) -> Column:
    """
    Converts an array of probabilities to hard calls

    Added in version 0.3.0.

    Examples:
        >>> df = spark.createDataFrame([Row(probs=[0.95, 0.05, 0.0])])
        >>> df.select(glow.hard_calls('probs', numAlts=lit(1), phased=lit(False)).alias('calls')).collect()
        [Row(calls=[0, 0])]
        >>> df = spark.createDataFrame([Row(probs=[0.05, 0.95, 0.0])])
        >>> df.select(glow.hard_calls('probs', numAlts=lit(1), phased=lit(False)).alias('calls')).collect()
        [Row(calls=[1, 0])]
        >>> # Use the threshold parameter to change the minimum probability required for a call
        >>> df = spark.createDataFrame([Row(probs=[0.05, 0.95, 0.0])])
        >>> df.select(glow.hard_calls('probs', numAlts=lit(1), phased=lit(False), threshold=0.99).alias('calls')).collect()
        [Row(calls=[-1, -1])]

    Args:
    probabilities : Probabilities
    numAlts : The number of alts
    phased : Whether the probabilities are phased or not
    threshold : The minimum probability to include
    """
    assert check_argument_types()
    if threshold is None:
        output = Column(sc()._jvm.io.projectglow.functions.hard_calls(_to_java_column(probabilities), _to_java_column(numAlts), _to_java_column(phased)))
    else:
        output = Column(sc()._jvm.io.projectglow.functions.hard_calls(_to_java_column(probabilities), _to_java_column(numAlts), _to_java_column(phased), threshold))
    assert check_return_type(output)
    return output
  

def lift_over_coordinates(contigName: Union[Column, str], start: Union[Column, str], end: Union[Column, str], chainFile: str, minMatchRatio: float = None) -> Column:
    """
    Do liftover like Picard

    Added in version 0.3.0.

    Examples:
        >>> df = spark.read.format('vcf').load('test-data/liftover/unlifted.test.vcf').where('start = 18210071')
        >>> chain_file = 'test-data/liftover/hg38ToHg19.over.chain.gz'
        >>> reference_file = 'test-data/liftover/hg19.chr20.fa.gz'
        >>> df.select('contigName', 'start', 'end').head()
        Row(contigName='chr20', start=18210071, end=18210072)
        >>> lifted_df = df.select(glow.expand_struct(glow.lift_over_coordinates('contigName', 'start', 'end', chain_file)))
        >>> lifted_df.head()
        Row(contigName='chr20', start=18190715, end=18190716)

    Args:
    contigName : The current contigName
    start : The current start
    end : The current end
    chainFile : Location of the chain file on each node in the cluster
    minMatchRatio : Minimum fraction of bases that must remap to lift over successfully
    """
    assert check_argument_types()
    if minMatchRatio is None:
        output = Column(sc()._jvm.io.projectglow.functions.lift_over_coordinates(_to_java_column(contigName), _to_java_column(start), _to_java_column(end), chainFile))
    else:
        output = Column(sc()._jvm.io.projectglow.functions.lift_over_coordinates(_to_java_column(contigName), _to_java_column(start), _to_java_column(end), chainFile, minMatchRatio))
    assert check_return_type(output)
    return output
  
