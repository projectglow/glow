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

    Parameters
    ----------
    struct : The struct to which fields will be added
    fields : New fields

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.add_struct_fields(_to_java_column(struct), _to_seq(fields)))
    assert check_return_type(output)
    return output
  

def array_summary_stats(arr: Union[Column, str]) -> Column:
    """
    Compute the min, max, mean, stddev for an array of numerics

    Parameters
    ----------
    arr : The array of numerics

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.array_summary_stats(_to_java_column(arr)))
    assert check_return_type(output)
    return output
  

def array_to_dense_vector(arr: Union[Column, str]) -> Column:
    """
    Convert an array of numerics into a spark.ml DenseVector

    Parameters
    ----------
    arr : The array of numerics

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.array_to_dense_vector(_to_java_column(arr)))
    assert check_return_type(output)
    return output
  

def array_to_sparse_vector(arr: Union[Column, str]) -> Column:
    """
    Convert an array of numerics into a spark.ml SparseVector

    Parameters
    ----------
    arr : The array of numerics

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.array_to_sparse_vector(_to_java_column(arr)))
    assert check_return_type(output)
    return output
  

def expand_struct(struct: Union[Column, str]) -> Column:
    """
    Promote fields of a nested struct to top-level columns. Similar to using struct.* from SQL, but can be used in more contexts.

    Parameters
    ----------
    struct : The struct to expand

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.expand_struct(_to_java_column(struct)))
    assert check_return_type(output)
    return output
  

def explode_matrix(matrix: Union[Column, str]) -> Column:
    """
    Explode a spark.ml Matrix into arrays of rows

    Parameters
    ----------
    matrix : The matrix to explode

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.explode_matrix(_to_java_column(matrix)))
    assert check_return_type(output)
    return output
  

def subset_struct(struct: Union[Column, str], *fields: str) -> Column:
    """
    Select fields from a struct

    Parameters
    ----------
    struct : Struct from which to select fields
    fields : Fields to take

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.subset_struct(_to_java_column(struct), _to_seq(fields)))
    assert check_return_type(output)
    return output
  

def vector_to_array(vector: Union[Column, str]) -> Column:
    """
    Convert a spark.ml vector (sparse or dense) to an array of doubles

    Parameters
    ----------
    vector : Vector to convert

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.vector_to_array(_to_java_column(vector)))
    assert check_return_type(output)
    return output
  
########### etl

def hard_calls(probabilities: Union[Column, str], numAlts: Union[Column, str], phased: Union[Column, str], threshold: float = None) -> Column:
    """
    Converts an array of probabilities to hard calls

    Parameters
    ----------
    probabilities : Probabilities
    numAlts : The number of alts
    phased : Whether the probabilities are phased or not
    threshold : The minimum probability to include

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    if threshold is None:
        output = Column(sc()._jvm.io.projectglow.functions.hard_calls(_to_java_column(probabilities), _to_java_column(numAlts), _to_java_column(phased)))
    else:
        output = Column(sc()._jvm.io.projectglow.functions.hard_calls(_to_java_column(probabilities), _to_java_column(numAlts), _to_java_column(phased), float(threshold)))
    assert check_return_type(output)
    return output
  

def lift_over_coordinates(contigName: Union[Column, str], start: Union[Column, str], end: Union[Column, str], chainFile: str, minMatchRatio: float = None) -> Column:
    """
    Do liftover like Picard

    Parameters
    ----------
    contigName : The current contigName
    start : The current start
    end : The current end
    chainFile : Location of the chain file on each node in the cluster
    minMatchRatio : Minimum fraction of bases that must remap to lift over successfully

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    if minMatchRatio is None:
        output = Column(sc()._jvm.io.projectglow.functions.lift_over_coordinates(_to_java_column(contigName), _to_java_column(start), _to_java_column(end), str(chainFile)))
    else:
        output = Column(sc()._jvm.io.projectglow.functions.lift_over_coordinates(_to_java_column(contigName), _to_java_column(start), _to_java_column(end), str(chainFile), float(minMatchRatio)))
    assert check_return_type(output)
    return output
  

def normalize_variant(contigName: Union[Column, str], start: Union[Column, str], end: Union[Column, str], refAllele: Union[Column, str], altAlleles: Union[Column, str], refGenomePathString: str) -> Column:
    """
    Normalize the variant using algorithms similar to vt normalize or bcftools norm.
    Creates a StructType column including the normalized start, end, referenceAllele and
    alternateAlleles fields (whether changed or unchanged) as well as a StructType field
    called normalizationStatus that contains the follwoing fields:

    changed: A boolean fields whether the variant data was changed as a result of normalization.

    errorMessage: An error message in case the attempt at normalizing the row hit an
        error. In this case, the changed field will be set to false. If no errors occur
        this field will be null.

    In case of error, the start, end, referemnceAllele and alternateAlleles fields in
    the generated struct will be null.

    Parameters
    ----------
    contigName : The current contigName
    start : The current start
    end : The current end
    refAllele : Reference allele
    altAlleles : Alternate alleles
    refGenomePathString : A path to the reference genome .fasta file. The .fasta file must
        be accompanied with a .fai index file in the same folder.

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.normalize_variant(_to_java_column(contigName), _to_java_column(start), _to_java_column(end), _to_java_column(refAllele), _to_java_column(altAlleles), str(refGenomePathString)))
    assert check_return_type(output)
    return output
  
########### quality_control

def call_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Compute call stats for an array of genotype structs

    Parameters
    ----------
    genotypes : The array of genotype structs

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.call_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output
  

def dp_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Compute summary statistics for depth field from array of genotype structs

    Parameters
    ----------
    genotypes : The array of genotype structs

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.dp_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output
  

def hardy_weinberg(genotypes: Union[Column, str]) -> Column:
    """
    Compute statistics relating to the Hardy Weinberg equilibrium

    Parameters
    ----------
    genotypes : The array of genotype structs

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.hardy_weinberg(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output
  

def gq_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Compute summary statistics about the genotype quality field for an array of genotype structs

    Parameters
    ----------
    genotypes : The array of genotype structs

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.gq_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output
  

def sample_call_summary_stats(genotypes: Union[Column, str], refAllele: Union[Column, str], alternateAlleles: Union[Column, str]) -> Column:
    """
    Compute per-sample call stats

    Parameters
    ----------
    genotypes : The array of genotype structs
    refAllele : The reference allele
    alternateAlleles : An array of alternate alleles

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.sample_call_summary_stats(_to_java_column(genotypes), _to_java_column(refAllele), _to_java_column(alternateAlleles)))
    assert check_return_type(output)
    return output
  

def sample_dp_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Compute per-sample summary statistics about the depth field in an array of genotype structs

    Parameters
    ----------
    genotypes : The array of genotype structs

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.sample_dp_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output
  

def sample_gq_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Compute per-sample summary statistics about the genotype quality field in an array of genotype structs

    Parameters
    ----------
    genotypes : The array of genotype structs

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.sample_gq_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output
  
########### gwas_functions

def linear_regression_gwas(genotypes: Union[Column, str], phenotypes: Union[Column, str], covariates: Union[Column, str]) -> Column:
    """
    A linear regression GWAS function

    Parameters
    ----------
    genotypes : An array of genotypes
    phenotypes : An array of phenotypes
    covariates : A Spark matrix of covariates

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.linear_regression_gwas(_to_java_column(genotypes), _to_java_column(phenotypes), _to_java_column(covariates)))
    assert check_return_type(output)
    return output
  

def logistic_regression_gwas(genotypes: Union[Column, str], phenotypes: Union[Column, str], covariates: Union[Column, str], test: str) -> Column:
    """
    A logistic regression function

    Parameters
    ----------
    genotypes : An array of genotypes
    phenotypes : An array of phenotype values
    covariates : a matrix of covariates
    test : Which logistic regression test to use. Can be 'LRG' or 'Firth'

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.logistic_regression_gwas(_to_java_column(genotypes), _to_java_column(phenotypes), _to_java_column(covariates), str(test)))
    assert check_return_type(output)
    return output
  

def genotype_states(genotypes: Union[Column, str]) -> Column:
    """
    Get number of alt alleles for a genotype

    Parameters
    ----------
    genotypes : An array of genotype structs

    .. versionadded 0.3.0
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.genotype_states(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output
  
