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

    Examples:
        >>> spark.createDataFrame([Row(struct=Row(a=1))]).select(add_struct_fields('struct', lit('b'), lit(2)).alias('struct')).collect()
        [Row(struct=Row(a=1, b=2))]


    Args:
        struct: The struct to which fields will be added
        fields: New fields
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.add_struct_fields(_to_java_column(struct), _to_seq(sc(), fields, _to_java_column)))
    assert check_return_type(output)
    return output
  

def array_summary_stats(arr: Union[Column, str]) -> Column:
    """
    Compute the min, max, mean, stddev for an array of numerics

    Examples:
        >>> spark.createDataFrame([Row(arr=[1, 2, 3])]).select(expand_struct(array_summary_stats('arr'))).collect()
        [Row(mean=2.0, stdDev=1.0, min=1.0, max=3.0)]


    Args:
        arr: The array of numerics
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.array_summary_stats(_to_java_column(arr)))
    assert check_return_type(output)
    return output
  

def array_to_dense_vector(arr: Union[Column, str]) -> Column:
    """
    Convert an array of numerics into a spark.ml DenseVector

    Examples:
        >>> from pyspark.ml.linalg import DenseVector
        >>> spark.createDataFrame([Row(arr=[1, 2, 3])]).select(array_to_dense_vector('arr').alias('v')).collect()
        [Row(v=DenseVector([1.0, 2.0, 3.0]))]


    Args:
        arr: The array of numerics
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.array_to_dense_vector(_to_java_column(arr)))
    assert check_return_type(output)
    return output
  

def array_to_sparse_vector(arr: Union[Column, str]) -> Column:
    """
    Convert an array of numerics into a spark.ml SparseVector

    Examples:
        >>> from pyspark.ml.linalg import SparseVector
        >>> spark.createDataFrame([Row(arr=[1, 0, 2, 0, 3, 0])]).select(array_to_sparse_vector('arr').alias('v')).collect()
        [Row(v=SparseVector(6, {0: 1.0, 2: 2.0, 4: 3.0}))]


    Args:
        arr: The array of numerics
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.array_to_sparse_vector(_to_java_column(arr)))
    assert check_return_type(output)
    return output
  

def expand_struct(struct: Union[Column, str]) -> Column:
    """
    Promote fields of a nested struct to top-level columns. Similar to using struct.* from SQL, but can be used in more contexts.

    Examples:
        >>> spark.createDataFrame([Row(struct=Row(a=1, b=2))]).select(expand_struct(col('struct'))).collect()
        [Row(a=1, b=2)]


    Args:
        struct: The struct to expand
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.expand_struct(_to_java_column(struct)))
    assert check_return_type(output)
    return output
  

def explode_matrix(matrix: Union[Column, str]) -> Column:
    """
    Explode a spark.ml Matrix into arrays of rows

    Examples:
        >>> from pyspark.ml.linalg import DenseMatrix
        >>> m = DenseMatrix(numRows=3, numCols=2, values=[1, 2, 3, 4, 5, 6])
        >>> spark.createDataFrame([Row(matrix=m)]).select(explode_matrix('matrix').alias('row')).collect()
        [Row(row=[1.0, 4.0]), Row(row=[2.0, 5.0]), Row(row=[3.0, 6.0])]


    Args:
        matrix: The matrix to explode
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.explode_matrix(_to_java_column(matrix)))
    assert check_return_type(output)
    return output
  

def subset_struct(struct: Union[Column, str], *fields: str) -> Column:
    """
    Select fields from a struct

    Examples:
        >>> spark.createDataFrame([Row(struct=Row(a=1, b=2, c=3))]).select(subset_struct('struct', 'a', 'c').alias('struct')).collect()
        [Row(struct=Row(a=1, c=3))]


    Args:
        struct: Struct from which to select fields
        fields: Fields to take
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.subset_struct(_to_java_column(struct), _to_seq(sc(), fields)))
    assert check_return_type(output)
    return output
  
########### etl

def hard_calls(probabilities: Union[Column, str], numAlts: Union[Column, str], phased: Union[Column, str], threshold: float = None) -> Column:
    """
    Converts an array of probabilities to hard calls

    Examples:
        >>> spark.createDataFrame([Row(probs=[0.95, 0.05, 0.0])]).select(hard_calls('probs', numAlts=lit(1), phased=lit(False)).alias('calls')).collect()
        [Row(calls=[0, 0])]


    Args:
        probabilities: Probabilities
        numAlts: The number of alts
        phased: Whether the probabilities are phased or not
        threshold: The minimum probability to include
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

    Examples:
        

    Args:
        contigName: The current contigName
        start: The current start
        end: The current end
        chainFile: Location of the chain file on each node in the cluster
        minMatchRatio: Minimum fraction of bases that must remap to lift over successfully
    """
    assert check_argument_types()
    if minMatchRatio is None:
        output = Column(sc()._jvm.io.projectglow.functions.lift_over_coordinates(_to_java_column(contigName), _to_java_column(start), _to_java_column(end), chainFile))
    else:
        output = Column(sc()._jvm.io.projectglow.functions.lift_over_coordinates(_to_java_column(contigName), _to_java_column(start), _to_java_column(end), chainFile, minMatchRatio))
    assert check_return_type(output)
    return output
  
########### quality_control

def call_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Compute call stats for an array of genotype structs

    Examples:
        >>> df = spark.createDataFrame([Row(genotypes=[Row(calls=[0, 0]), Row(calls=[1, 0]), Row(calls=[1, 1])])], 'genotypes: array<struct<calls: array<int>>>')
        >>> df.select(expand_struct(call_summary_stats('genotypes'))).collect()
        [Row(callRate=1.0, nCalled=3, nUncalled=0, nHet=1, nHomozygous=[1, 1], nNonRef=2, nAllelesCalled=6, alleleCounts=[3, 3], alleleFrequencies=[0.5, 0.5])]


    Args:
        genotypes: The array of genotype structs
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.call_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output
  

def dp_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Compute summary statistics for depth field from array of genotype structs

    Examples:
        

    Args:
        genotypes: The array of genotype structs
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.dp_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output
  

def hardy_weinberg(genotypes: Union[Column, str]) -> Column:
    """
    Compute statistics relating to the Hardy Weinberg equilibrium

    Examples:
        

    Args:
        genotypes: The array of genotype structs
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.hardy_weinberg(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output
  

def gq_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Compute summary statistics about the genotype quality field for an array of genotype structs

    Examples:
        

    Args:
        genotypes: The array of genotype structs
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.gq_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output
  

def sample_call_summary_stats(genotypes: Union[Column, str], refAllele: Union[Column, str], alternateAlleles: Union[Column, str]) -> Column:
    """
    Compute per-sample call stats

    Examples:
        

    Args:
        genotypes: The array of genotype structs
        refAllele: The reference allele
        alternateAlleles: An array of alternate alleles
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.sample_call_summary_stats(_to_java_column(genotypes), _to_java_column(refAllele), _to_java_column(alternateAlleles)))
    assert check_return_type(output)
    return output
  
########### gwas_functions

def linear_regression_gwas(genotypes: Union[Column, str], phenotypes: Union[Column, str], covariates: Union[Column, str]) -> Column:
    """
    A linear regression GWAS function

    Examples:
        

    Args:
        genotypes: An array of genotypes
        phenotypes: An array of phenotypes
        covariates: A Spark matrix of covariates
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.linear_regression_gwas(_to_java_column(genotypes), _to_java_column(phenotypes), _to_java_column(covariates)))
    assert check_return_type(output)
    return output
  

def logistic_regression_gwas(genotypes: Union[Column, str], phenotypes: Union[Column, str], covariates: Union[Column, str], test: str) -> Column:
    """
    A logistic regression function

    Examples:
        

    Args:
        genotypes: An array of genotypes
        phenotypes: An array of phenotype values
        covariates: a matrix of covariates
        test: Which logistic regression test to use. Can be 'LRG' or 'Firth'
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.logistic_regression_gwas(_to_java_column(genotypes), _to_java_column(phenotypes), _to_java_column(covariates), test))
    assert check_return_type(output)
    return output
  

def genotype_states(genotypes: Union[Column, str]) -> Column:
    """
    Get number of alt alleles for a genotype

    Examples:
        

    Args:
        genotypes: An array of genotype structs
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.genotype_states(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output
  
