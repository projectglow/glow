# Copyright 2019 The Glow Authors
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
    Adds fields to a struct.

    Added in version 0.3.0.

    Examples:
        >>> df = spark.createDataFrame([Row(struct=Row(a=1))])
        >>> df.select(glow.add_struct_fields('struct', lit('b'), lit(2)).alias('struct')).collect()
        [Row(struct=Row(a=1, b=2))]

    Args:
        struct : The struct to which fields will be added
        fields : The new fields to add. The arguments must alternate between string-typed literal field names and field values.

    Returns:
        A struct consisting of the input struct and the added fields
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.add_struct_fields(_to_java_column(struct), _to_seq(sc(), fields, _to_java_column)))
    assert check_return_type(output)
    return output


def array_summary_stats(arr: Union[Column, str]) -> Column:
    """
    Computes the minimum, maximum, mean, standard deviation for an array of numerics.

    Added in version 0.3.0.

    Examples:
        >>> df = spark.createDataFrame([Row(arr=[1, 2, 3])])
        >>> df.select(glow.expand_struct(glow.array_summary_stats('arr'))).collect()
        [Row(mean=2.0, stdDev=1.0, min=1.0, max=3.0)]

    Args:
        arr : An array of any numeric type

    Returns:
        A struct containing double ``mean``, ``stdDev``, ``min``, and ``max`` fields
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.array_summary_stats(_to_java_column(arr)))
    assert check_return_type(output)
    return output


def array_to_dense_vector(arr: Union[Column, str]) -> Column:
    """
    Converts an array of numerics into a ``spark.ml`` ``DenseVector``.

    Added in version 0.3.0.

    Examples:
        >>> from pyspark.ml.linalg import DenseVector
        >>> df = spark.createDataFrame([Row(arr=[1, 2, 3])])
        >>> df.select(glow.array_to_dense_vector('arr').alias('v')).collect()
        [Row(v=DenseVector([1.0, 2.0, 3.0]))]

    Args:
        arr : The array of numerics

    Returns:
        A ``spark.ml`` ``DenseVector``
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.array_to_dense_vector(_to_java_column(arr)))
    assert check_return_type(output)
    return output


def array_to_sparse_vector(arr: Union[Column, str]) -> Column:
    """
    Converts an array of numerics into a ``spark.ml`` ``SparseVector``.

    Added in version 0.3.0.

    Examples:
        >>> from pyspark.ml.linalg import SparseVector
        >>> df = spark.createDataFrame([Row(arr=[1, 0, 2, 0, 3, 0])])
        >>> df.select(glow.array_to_sparse_vector('arr').alias('v')).collect()
        [Row(v=SparseVector(6, {0: 1.0, 2: 2.0, 4: 3.0}))]

    Args:
        arr : The array of numerics

    Returns:
        A ``spark.ml`` ``SparseVector``
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.array_to_sparse_vector(_to_java_column(arr)))
    assert check_return_type(output)
    return output


def expand_struct(struct: Union[Column, str]) -> Column:
    """
    Promotes fields of a nested struct to top-level columns similar to using ``struct.*`` from SQL, but can be used in more contexts.

    Added in version 0.3.0.

    Examples:
        >>> df = spark.createDataFrame([Row(struct=Row(a=1, b=2))])
        >>> df.select(glow.expand_struct(col('struct'))).collect()
        [Row(a=1, b=2)]

    Args:
        struct : The struct to expand

    Returns:
        Columns corresponding to fields of the input struct
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.expand_struct(_to_java_column(struct)))
    assert check_return_type(output)
    return output


def explode_matrix(matrix: Union[Column, str]) -> Column:
    """
    Explodes a ``spark.ml`` ``Matrix`` (sparse or dense) into multiple arrays, one per row of the matrix.

    Added in version 0.3.0.

    Examples:
        >>> from pyspark.ml.linalg import DenseMatrix
        >>> m = DenseMatrix(numRows=3, numCols=2, values=[1, 2, 3, 4, 5, 6])
        >>> df = spark.createDataFrame([Row(matrix=m)])
        >>> df.select(glow.explode_matrix('matrix').alias('row')).collect()
        [Row(row=[1.0, 4.0]), Row(row=[2.0, 5.0]), Row(row=[3.0, 6.0])]

    Args:
        matrix : The ``sparl.ml`` ``Matrix`` to explode

    Returns:
        An array column in which each row is a row of the input matrix
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.explode_matrix(_to_java_column(matrix)))
    assert check_return_type(output)
    return output


def subset_struct(struct: Union[Column, str], *fields: str) -> Column:
    """
    Selects fields from a struct.

    Added in version 0.3.0.

    Examples:
        >>> df = spark.createDataFrame([Row(struct=Row(a=1, b=2, c=3))])
        >>> df.select(glow.subset_struct('struct', 'a', 'c').alias('struct')).collect()
        [Row(struct=Row(a=1, c=3))]

    Args:
        struct : Struct from which to select fields
        fields : Fields to select

    Returns:
        A struct containing only the indicated fields
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.subset_struct(_to_java_column(struct), _to_seq(sc(), fields)))
    assert check_return_type(output)
    return output


def vector_to_array(vector: Union[Column, str]) -> Column:
    """
    Converts a ``spark.ml`` ``Vector`` (sparse or dense) to an array of doubles.

    Added in version 0.3.0.

    Examples:
        >>> from pyspark.ml.linalg import DenseVector, SparseVector
        >>> df = spark.createDataFrame([Row(v=SparseVector(3, {0: 1.0, 2: 2.0})), Row(v=DenseVector([3.0, 4.0]))])
        >>> df.select(glow.vector_to_array('v').alias('arr')).collect()
        [Row(arr=[1.0, 0.0, 2.0]), Row(arr=[3.0, 4.0])]

    Args:
        vector : Vector to convert

    Returns:
        An array of doubles
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.vector_to_array(_to_java_column(vector)))
    assert check_return_type(output)
    return output

########### etl

def hard_calls(probabilities: Union[Column, str], numAlts: Union[Column, str], phased: Union[Column, str], threshold: float = None) -> Column:
    """
    Converts an array of probabilities to hard calls. The probabilities are assumed to be diploid. See :ref:`variant-data-transformations` for more details.

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
        probabilities : The array of probabilities to convert
        numAlts : The number of alternate alleles
        phased : Whether the probabilities are phased. If phased, we expect one ``2 * numAlts`` values in the probabilities array. If unphased, we expect one probability per possible genotype.
        threshold : The minimum probability to make a call. If no probability falls into the range of ``[0, 1 - threshold]`` or ``[threshold, 1]``, a no-call (represented by ``-1`` s) will be emitted. If not provided, this parameter defaults to ``0.9``.

    Returns:
        An array of hard calls
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
    Performs liftover for the coordinates of a variant. To perform liftover of alleles and add additional metadata, see :ref:`liftover`.

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
        contigName : The current contig name
        start : The current start
        end : The current end
        chainFile : Location of the chain file on each node in the cluster
        minMatchRatio : Minimum fraction of bases that must remap to do liftover successfully. If not provided, defaults to ``0.95``.

    Returns:
        A struct containing ``contigName``, ``start``, and ``end`` fields after liftover
    """
    assert check_argument_types()
    if minMatchRatio is None:
        output = Column(sc()._jvm.io.projectglow.functions.lift_over_coordinates(_to_java_column(contigName), _to_java_column(start), _to_java_column(end), chainFile))
    else:
        output = Column(sc()._jvm.io.projectglow.functions.lift_over_coordinates(_to_java_column(contigName), _to_java_column(start), _to_java_column(end), chainFile, minMatchRatio))
    assert check_return_type(output)
    return output


def normalize_variant(contigName: Union[Column, str], start: Union[Column, str], end: Union[Column, str], refAllele: Union[Column, str], altAlleles: Union[Column, str], refGenomePathString: str) -> Column:
    """
    Normalizes the variant with a behavior similar to vt normalize or bcftools norm.
    Creates a StructType column including the normalized ``start``, ``end``, ``referenceAllele`` and
    ``alternateAlleles`` fields (whether they are changed or unchanged as the result of
    normalization) as well as a StructType field called ``normalizationStatus`` that
    contains the following fields:

       ``changed``: A boolean field indicating whether the variant data was changed as a result of normalization

       ``errorMessage``: An error message in case the attempt at normalizing the row hit an error. In this case, the ``changed`` field will be set to ``false``. If no errors occur, this field will be ``null``.

    In case of an error, the ``start``, ``end``, ``referenceAllele`` and ``alternateAlleles`` fields in the generated struct will be ``null``.

    Added in version 0.3.0.

    Examples:
        >>> df = spark.read.format('vcf').load('test-data/variantsplitternormalizer-test/test_left_align_hg38_altered.vcf')
        >>> ref_genome = 'test-data/variantsplitternormalizer-test/Homo_sapiens_assembly38.20.21_altered.fasta'
        >>> df.select('contigName', 'start', 'end', 'referenceAllele', 'alternateAlleles').head()
        Row(contigName='chr20', start=400, end=401, referenceAllele='G', alternateAlleles=['GATCTTCCCTCTTTTCTAATATAAACACATAAAGCTCTGTTTCCTTCTAGGTAACTGGTTTGAG'])
        >>> normalized_df = df.select('contigName', glow.expand_struct(glow.normalize_variant('contigName', 'start', 'end', 'referenceAllele', 'alternateAlleles', ref_genome)))
        >>> normalized_df.head()
        Row(contigName='chr20', start=268, end=269, referenceAllele='A', alternateAlleles=['ATTTGAGATCTTCCCTCTTTTCTAATATAAACACATAAAGCTCTGTTTCCTTCTAGGTAACTGG'], normalizationStatus=Row(changed=True, errorMessage=None))

    Args:
        contigName : The current contig name
        start : The current start
        end : The current end
        refAllele : The current reference allele
        altAlleles : The current array of alternate alleles
        refGenomePathString : A path to the reference genome ``.fasta`` file. The ``.fasta`` file must be accompanied with a ``.fai`` index file in the same folder.

    Returns:
        A struct as explained above
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.normalize_variant(_to_java_column(contigName), _to_java_column(start), _to_java_column(end), _to_java_column(refAllele), _to_java_column(altAlleles), refGenomePathString))
    assert check_return_type(output)
    return output


def mean_substitute(array: Union[Column, str], missingValue: Union[Column, str] = None) -> Column:
    """
    Substitutes the missing values of a numeric array using the mean of the non-missing values. Any values that are NaN, null or equal to the missing value parameter are considered missing. See :ref:`variant-data-transformations` for more details.

    Added in version 0.4.0.

    Examples:
        >>> df = spark.createDataFrame([Row(unsubstituted_values=[float('nan'), None, 0.0, 1.0, 2.0, 3.0, 4.0])])
        >>> df.select(glow.mean_substitute('unsubstituted_values', lit(0.0)).alias('substituted_values')).collect()
        [Row(substituted_values=[2.5, 2.5, 2.5, 1.0, 2.0, 3.0, 4.0])]
        >>> df = spark.createDataFrame([Row(unsubstituted_values=[0, 1, 2, 3, -1, None])])
        >>> df.select(glow.mean_substitute('unsubstituted_values').alias('substituted_values')).collect()
        [Row(substituted_values=[0.0, 1.0, 2.0, 3.0, 1.5, 1.5])]

    Args:
        array : A numeric array that may contain missing values
        missingValue : A value that should be considered missing. If not provided, this parameter defaults to ``-1``.

    Returns:
        A numeric array with substituted missing values
    """
    assert check_argument_types()
    if missingValue is None:
        output = Column(sc()._jvm.io.projectglow.functions.mean_substitute(_to_java_column(array)))
    else:
        output = Column(sc()._jvm.io.projectglow.functions.mean_substitute(_to_java_column(array), _to_java_column(missingValue)))
    assert check_return_type(output)
    return output

########### quality_control

def call_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Computes call summary statistics for an array of genotype structs. See :ref:`variant-qc` for more details.

    Added in version 0.3.0.

    Examples:
        >>> schema = 'genotypes: array<struct<calls: array<int>>>'
        >>> df = spark.createDataFrame([Row(genotypes=[Row(calls=[0, 0]), Row(calls=[1, 0]), Row(calls=[1, 1])])], schema)
        >>> df.select(glow.expand_struct(glow.call_summary_stats('genotypes'))).collect()
        [Row(callRate=1.0, nCalled=3, nUncalled=0, nHet=1, nHomozygous=[1, 1], nNonRef=2, nAllelesCalled=6, alleleCounts=[3, 3], alleleFrequencies=[0.5, 0.5])]

    Args:
        genotypes : The array of genotype structs with ``calls`` field

    Returns:
        A struct containing ``callRate``, ``nCalled``, ``nUncalled``, ``nHet``, ``nHomozygous``, ``nNonRef``, ``nAllelesCalled``, ``alleleCounts``, ``alleleFrequencies`` fields. See :ref:`variant-qc`.
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.call_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output


def dp_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Computes summary statistics for the depth field from an array of genotype structs. See :ref:`variant-qc`.

    Added in version 0.3.0.

    Examples:
        >>> df = spark.createDataFrame([Row(genotypes=[Row(depth=1), Row(depth=2), Row(depth=3)])], 'genotypes: array<struct<depth: int>>')
        >>> df.select(glow.expand_struct(glow.dp_summary_stats('genotypes'))).collect()
        [Row(mean=2.0, stdDev=1.0, min=1.0, max=3.0)]

    Args:
        genotypes : An array of genotype structs with ``depth`` field

    Returns:
        A struct containing ``mean``, ``stdDev``, ``min``, and ``max`` of genotype depths
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.dp_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output


def hardy_weinberg(genotypes: Union[Column, str]) -> Column:
    """
    Computes statistics relating to the Hardy Weinberg equilibrium. See :ref:`variant-qc` for more details.

    Added in version 0.3.0.

    Examples:
        >>> genotypes = [
        ... Row(calls=[1, 1]),
        ... Row(calls=[1, 0]),
        ... Row(calls=[0, 0])]
        >>> df = spark.createDataFrame([Row(genotypes=genotypes)], 'genotypes: array<struct<calls: array<int>>>')
        >>> df.select(glow.expand_struct(glow.hardy_weinberg('genotypes'))).collect()
        [Row(hetFreqHwe=0.6, pValueHwe=0.7)]

    Args:
        genotypes : The array of genotype structs with ``calls`` field

    Returns:
        A struct containing two fields, ``hetFreqHwe`` (the expected heterozygous frequency according to Hardy-Weinberg equilibrium) and ``pValueHwe`` (the associated p-value)
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.hardy_weinberg(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output


def gq_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Computes summary statistics about the genotype quality field for an array of genotype structs. See :ref:`variant-qc`.

    Added in version 0.3.0.

    Examples:
        >>> genotypes = [
        ... Row(conditionalQuality=1), 
        ... Row(conditionalQuality=2), 
        ... Row(conditionalQuality=3)] 
        >>> df = spark.createDataFrame([Row(genotypes=genotypes)], 'genotypes: array<struct<conditionalQuality: int>>')
        >>> df.select(glow.expand_struct(glow.gq_summary_stats('genotypes'))).collect()
        [Row(mean=2.0, stdDev=1.0, min=1.0, max=3.0)]

    Args:
        genotypes : The array of genotype structs with ``conditionalQuality`` field

    Returns:
        A struct containing ``mean``, ``stdDev``, ``min``, and ``max`` of genotype qualities
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.gq_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output


def sample_call_summary_stats(genotypes: Union[Column, str], refAllele: Union[Column, str], alternateAlleles: Union[Column, str]) -> Column:
    """
    Computes per-sample call summary statistics. See :ref:`sample-qc` for more details.

    Added in version 0.3.0.

    Examples:
        >>> sites = [
        ... Row(refAllele='C', alternateAlleles=['G'], genotypes=[Row(sampleId='NA12878', calls=[0, 0])]),
        ... Row(refAllele='A', alternateAlleles=['G'], genotypes=[Row(sampleId='NA12878', calls=[1, 1])]),
        ... Row(refAllele='AG', alternateAlleles=['A'], genotypes=[Row(sampleId='NA12878', calls=[1, 0])])]
        >>> df = spark.createDataFrame(sites, 'alternateAlleles: array<string>, genotypes: array<struct<sampleId: string, calls: array<int>>>, refAllele: string')
        >>> df.select(glow.sample_call_summary_stats('genotypes', 'refAllele', 'alternateAlleles').alias('stats')).collect()
        [Row(stats=[Row(sampleId='NA12878', callRate=1.0, nCalled=3, nUncalled=0, nHomRef=1, nHet=1, nHomVar=1, nSnp=2, nInsertion=0, nDeletion=1, nTransition=2, nTransversion=0, nSpanningDeletion=0, rTiTv=inf, rInsertionDeletion=0.0, rHetHomVar=1.0)])]

    Args:
        genotypes : An array of genotype structs with ``calls`` field
        refAllele : The reference allele
        alternateAlleles : An array of alternate alleles

    Returns:
        A struct containing ``sampleId``, ``callRate``, ``nCalled``, ``nUncalled``, ``nHomRef``, ``nHet``, ``nHomVar``, ``nSnp``, ``nInsertion``, ``nDeletion``, ``nTransition``, ``nTransversion``, ``nSpanningDeletion``, ``rTiTv``, ``rInsertionDeletion``, ``rHetHomVar`` fields. See :ref:`sample-qc`.
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.sample_call_summary_stats(_to_java_column(genotypes), _to_java_column(refAllele), _to_java_column(alternateAlleles)))
    assert check_return_type(output)
    return output


def sample_dp_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Computes per-sample summary statistics about the depth field in an array of genotype structs.

    Added in version 0.3.0.

    Examples:
        >>> sites = [
        ... Row(genotypes=[Row(sampleId='NA12878', depth=1)]),
        ... Row(genotypes=[Row(sampleId='NA12878', depth=2)]),
        ... Row(genotypes=[Row(sampleId='NA12878', depth=3)])]
        >>> df = spark.createDataFrame(sites, 'genotypes: array<struct<depth: int, sampleId: string>>')
        >>> df.select(glow.sample_dp_summary_stats('genotypes').alias('stats')).collect()
        [Row(stats=[Row(sampleId='NA12878', mean=2.0, stdDev=1.0, min=1.0, max=3.0)])]

    Args:
        genotypes : An array of genotype structs with ``depth`` field

    Returns:
        An array of structs where each struct contains ``mean``, ``stDev``, ``min``, and ``max`` of the genotype depths for a sample. If ``sampleId`` is present in a genotype, it will be propagated to the resulting struct as an extra field.
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.sample_dp_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output


def sample_gq_summary_stats(genotypes: Union[Column, str]) -> Column:
    """
    Computes per-sample summary statistics about the genotype quality field in an array of genotype structs.

    Added in version 0.3.0.

    Examples:
        >>> sites = [
        ... Row(genotypes=[Row(sampleId='NA12878', conditionalQuality=1)]),
        ... Row(genotypes=[Row(sampleId='NA12878', conditionalQuality=2)]),
        ... Row(genotypes=[Row(sampleId='NA12878', conditionalQuality=3)])]
        >>> df = spark.createDataFrame(sites, 'genotypes: array<struct<conditionalQuality: int, sampleId: string>>')
        >>> df.select(glow.sample_gq_summary_stats('genotypes').alias('stats')).collect()
        [Row(stats=[Row(sampleId='NA12878', mean=2.0, stdDev=1.0, min=1.0, max=3.0)])]

    Args:
        genotypes : An array of genotype structs with ``conditionalQuality`` field

    Returns:
        An array of structs where each struct contains ``mean``, ``stDev``, ``min``, and ``max`` of the genotype qualities for a sample. If ``sampleId`` is present in a genotype, it will be propagated to the resulting struct as an extra field.
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.sample_gq_summary_stats(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output

########### gwas_functions

def linear_regression_gwas(genotypes: Union[Column, str], phenotypes: Union[Column, str], covariates: Union[Column, str]) -> Column:
    """
    Performs a linear regression association test optimized for performance in a GWAS setting. See :ref:`linear-regression` for details.

    Added in version 0.3.0.

    Examples:
        >>> from pyspark.ml.linalg import DenseMatrix
        >>> phenotypes = [2, 3, 4]
        >>> genotypes = [0, 1, 2]
        >>> covariates = DenseMatrix(numRows=3, numCols=1, values=[1, 1, 1])
        >>> df = spark.createDataFrame([Row(genotypes=genotypes, phenotypes=phenotypes, covariates=covariates)])
        >>> df.select(glow.expand_struct(glow.linear_regression_gwas('genotypes', 'phenotypes', 'covariates'))).collect()
        [Row(beta=0.9999999999999998, standardError=1.4901161193847656e-08, pValue=9.486373847239922e-09)]

    Args:
        genotypes : A numeric array of genotypes
        phenotypes : A numeric array of phenotypes
        covariates : A ``spark.ml`` ``Matrix`` of covariates

    Returns:
        A struct containing ``beta``, ``standardError``, and ``pValue`` fields. See :ref:`linear-regression`.
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.linear_regression_gwas(_to_java_column(genotypes), _to_java_column(phenotypes), _to_java_column(covariates)))
    assert check_return_type(output)
    return output


def logistic_regression_gwas(genotypes: Union[Column, str], phenotypes: Union[Column, str], covariates: Union[Column, str], test: str) -> Column:
    """
    Performs a logistic regression association test optimized for performance in a GWAS setting. See :ref:`logistic-regression` for more details.

    Added in version 0.3.0.

    Examples:
        >>> from pyspark.ml.linalg import DenseMatrix
        >>> phenotypes = [1, 0, 0, 1, 1]
        >>> genotypes = [0, 0, 1, 2, 2]
        >>> covariates = DenseMatrix(numRows=5, numCols=1, values=[1, 1, 1, 1, 1])
        >>> df = spark.createDataFrame([Row(genotypes=genotypes, phenotypes=phenotypes, covariates=covariates)])
        >>> df.select(glow.expand_struct(glow.logistic_regression_gwas('genotypes', 'phenotypes', 'covariates', 'Firth'))).collect()
        [Row(beta=0.7418937644793101, oddsRatio=2.09990848346903, waldConfidenceInterval=[0.2509874689201784, 17.569066925598555], pValue=0.3952193664793294)]
        >>> df.select(glow.expand_struct(glow.logistic_regression_gwas('genotypes', 'phenotypes', 'covariates', 'LRT'))).collect()
        [Row(beta=1.1658962684583645, oddsRatio=3.208797540870915, waldConfidenceInterval=[0.2970960052553798, 34.65674891673014], pValue=0.2943946848756771)]

    Args:
        genotypes : An numeric array of genotypes
        phenotypes : A double array of phenotype values
        covariates : A ``spark.ml`` ``Matrix`` of covariates
        test : Which logistic regression test to use. Can be ``LRT`` or ``Firth``

    Returns:
        A struct containing ``beta``, ``oddsRatio``, ``waldConfidenceInterval``, and ``pValue`` fields. See :ref:`logistic-regression`.
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.logistic_regression_gwas(_to_java_column(genotypes), _to_java_column(phenotypes), _to_java_column(covariates), test))
    assert check_return_type(output)
    return output


def genotype_states(genotypes: Union[Column, str]) -> Column:
    """
    Gets the number of alternate alleles for an array of genotype structs. Returns ``-1`` if there are any ``-1`` s (no-calls) in the calls array.

    Added in version 0.3.0.

    Examples:
        >>> genotypes = [
        ... Row(calls=[1, 1]),
        ... Row(calls=[1, 0]),
        ... Row(calls=[0, 0]),
        ... Row(calls=[-1, -1])]
        >>> df = spark.createDataFrame([Row(genotypes=genotypes)], 'genotypes: array<struct<calls: array<int>>>')
        >>> df.select(glow.genotype_states('genotypes').alias('states')).collect()
        [Row(states=[2, 1, 0, -1])]

    Args:
        genotypes : An array of genotype structs with ``calls`` field

    Returns:
        An array of integers containing the number of alternate alleles in each call array
    """
    assert check_argument_types()
    output = Column(sc()._jvm.io.projectglow.functions.genotype_states(_to_java_column(genotypes)))
    assert check_return_type(output)
    return output

