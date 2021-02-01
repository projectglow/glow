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

from glow import glow
import pandas as pd
import numpy as np
from pyspark import SparkContext
from pyspark.sql import DataFrame, Row, SparkSession, SQLContext
from typeguard import check_argument_types, check_return_type
from typing import Any, Dict, List
from nptyping import Float, NDArray
from .ridge_reduction import RidgeReduction
from .ridge_regression import RidgeRegression
from .logistic_ridge_regression import LogisticRidgeRegression

__all__ = [
    'get_sample_ids', 'block_variants_and_samples', 'reshape_for_gwas', 'estimate_loco_offsets'
]


def _get_contigs_from_loco_df(df: pd.DataFrame) -> pd.Series:
    return df.index.get_level_values(1).unique()


def __validate_sample_ids(sample_ids: List[str]):
    """"
    Validates that a set of sample IDs are valid (non-empty and unique).
    """
    assert check_argument_types()
    if any(not s for s in sample_ids):
        raise Exception("Cannot have empty sample IDs.")
    if len(sample_ids) != len(set(sample_ids)):
        raise Exception("Cannot have duplicated sample IDs.")


def __get_index_map(sample_ids: List[str], sample_block_count: int,
                    sql_ctx: SQLContext) -> Dict[str, List[str]]:
    """
    Creates an index mapping from sample blocks to a list of corresponding sample IDs. Uses the same sample-blocking
    logic as the blocked GT matrix transformer.

    Requires that:
        - Each variant row has the same number of values
        - The number of values per row matches the number of sample IDs

    Args:
        sample_ids : The list of sample ID strings
        sample_block_count : The number of sample blocks

    Returns:
        index mapping from sample block IDs to a list of sample IDs
    """

    assert check_argument_types()

    sample_id_df = sql_ctx.createDataFrame([Row(values=sample_ids)])
    make_sample_blocks_fn = SparkContext._jvm.io.projectglow.transformers.blockvariantsandsamples.VariantSampleBlockMaker.makeSampleBlocks
    output_jdf = make_sample_blocks_fn(sample_id_df._jdf, sample_block_count)
    output_df = DataFrame(output_jdf, sql_ctx)
    index_map = {r.sample_block: r.values for r in output_df.collect()}

    assert check_return_type(index_map)
    return index_map


def get_sample_ids(data: DataFrame) -> List[str]:
    """
    Extracts sample IDs from a variant DataFrame, such as one read from PLINK files.

    Requires that the sample IDs:

        - Are in ``genotype.sampleId``
        - Are the same across all the variant rows
        - Are a list of strings
        - Are non-empty
        - Are unique

    Args:
        data : The variant DataFrame containing sample IDs

    Returns:
        list of sample ID strings
    """
    assert check_argument_types()
    distinct_sample_id_sets = data.selectExpr("genotypes.sampleId as sampleIds").distinct()
    if distinct_sample_id_sets.count() != 1:
        raise Exception("Each row must have the same set of sample IDs.")
    sample_ids = distinct_sample_id_sets.head().sampleIds
    __validate_sample_ids(sample_ids)
    assert check_return_type(sample_ids)
    return sample_ids


def block_variants_and_samples(variant_df: DataFrame, sample_ids: List[str],
                               variants_per_block: int,
                               sample_block_count: int) -> (DataFrame, Dict[str, List[str]]):
    """
    Creates a blocked GT matrix and index mapping from sample blocks to a list of corresponding sample IDs. Uses the
    same sample-blocking logic as the blocked GT matrix transformer.

    Requires that:

    - Each variant row has the same number of values
    - The number of values per row matches the number of sample IDs

    Args:
        variant_df : The variant DataFrame
        sample_ids : The list of sample ID strings
        variants_per_block : The number of variants per block
        sample_block_count : The number of sample blocks

    Returns:
        tuple of (blocked GT matrix, index mapping)
    """
    assert check_argument_types()
    first_row = variant_df.selectExpr("size(values) as numValues").take(1)
    if not first_row:
        raise Exception("DataFrame has no values.")
    num_values = first_row[0].numValues
    if num_values != len(sample_ids):
        raise Exception(
            f"Number of values does not match between DataFrame ({num_values}) and sample ID list ({len(sample_ids)})."
        )
    __validate_sample_ids(sample_ids)

    blocked_gt = glow.transform("block_variants_and_samples",
                                variant_df,
                                variants_per_block=variants_per_block,
                                sample_block_count=sample_block_count)
    index_map = __get_index_map(sample_ids, sample_block_count, variant_df.sql_ctx)

    output = blocked_gt, index_map
    assert check_return_type(output)
    return output


def reshape_for_gwas(spark: SparkSession, label_df: pd.DataFrame) -> DataFrame:
    """
    Reshapes a Pandas DataFrame into a Spark DataFrame with a convenient format for Glow's GWAS
    functions. This function can handle labels that are either per-sample or per-sample and
    per-contig, like those generated by GloWGR's transform_loco function.

    Examples:
        .. invisible-code-block:
            import pandas as pd

        >>> label_df = pd.DataFrame({'label1': [1, 2], 'label2': [3, 4]}, index=['sample1', 'sample2'])
        >>> reshaped = reshape_for_gwas(spark, label_df)
        >>> reshaped.head()
        Row(label='label1', values=[1, 2])

        >>> loco_label_df = pd.DataFrame({'label1': [1, 2], 'label2': [3, 4]},
        ...     index=pd.MultiIndex.from_tuples([('sample1', 'chr1'), ('sample1', 'chr2')]))
        >>> reshaped = reshape_for_gwas(spark, loco_label_df)
        >>> reshaped.head()
        Row(contigName='chr1', label='label1', values=[1])

    Requires that:

        - The input label DataFrame is indexed by sample id or by (sample id, contig name)

    Args:
        spark : A Spark session
        label_df : A pandas DataFrame containing labels. The Data Frame should either be indexed by
            sample id or multi indexed by (sample id, contig name). Each column is interpreted as a
            label.

    Returns:
        A Spark DataFrame with a convenient format for Glow regression functions. Each row contains
        the label name, the contig name if provided in the input DataFrame, and an array containing
        the label value for each sample.
    """

    assert check_argument_types()

    if label_df.index.nlevels == 1:  # Indexed by sample id
        transposed_df = label_df.T
        column_names = ['label', 'values']
    elif label_df.index.nlevels == 2:  # Indexed by sample id and contig name
        # stacking sorts the new column index, so we remember the original sample
        # ordering in case it's not sorted
        def transpose_one(contig):
            transposed = label_df.xs(contig, level=1).T
            return transposed

        contigs = _get_contigs_from_loco_df(label_df)
        transposed_df = pd.concat([transpose_one(contig) for contig in contigs],
                                  keys=contigs,
                                  names=['contig', 'label'])
        column_names = ['contigName', 'label', 'values']
    else:
        raise ValueError('label_df must be indexed by sample id or by (sample id, contig name)')

    # Can only create a Spark DataFrame from pandas with ndarray columns in Spark 3+
    if int(spark.version.split('.')[0]) < 3:
        values = transposed_df.to_numpy().tolist()
    else:
        values = list(transposed_df.to_numpy())
    transposed_df['values_array'] = values
    return spark.createDataFrame(transposed_df[['values_array']].reset_index(), column_names)


def estimate_loco_offsets(block_df: DataFrame,
                          label_df: pd.DataFrame,
                          sample_blocks: Dict[str, List[str]],
                          cov_df: pd.DataFrame = pd.DataFrame({}),
                          add_intercept: bool = True,
                          reduction_alphas: NDArray[(Any, ), Float] = np.array([]),
                          regression_alphas: NDArray[(Any, ), Float] = np.array([]),
                          label_type='detect',
                          chromosomes: List[str] = []) -> pd.DataFrame:
    """
    The one-stop function to generate WGR predictors to be used as offsets in gwas functions. Given the
    input the function performs the ridge reduction followed by appropriate choice of ridge regression or
    logistic ridge regression in a loco manner.

    Args:
        block_df : Spark DataFrame representing the beginning block matrix X
        label_df : Pandas DataFrame containing the target labels used in fitting the ridge models
        sample_blocks : Dict containing a mapping of sample_block ID to a list of corresponding sample IDs
        cov_df : Pandas DataFrame containing covariates to be included in every model in the stacking
            ensemble (optional).
        add_intercept: If True, an intercept column (all ones) will be added to the covariates
            (as the first column)
        reduction_alphas : array_like of alpha values used in the ridge reduction (optional). If not provided, the
            automatically generates alphas for reduction.
        regression_alphas : array_like of alpha values used in the ridge or logistic ridge regression (optional).
            If not provided, the automatically generates alphas for regression.
        label_type: String to determine type treatment of labels. It can be 'detect' (default), 'binary',
            or 'quantitative'. On 'detect' the function picks binary or quantitative based on whether label_df is all
            binary or not, respectively.
        chromosomes : List of chromosomes for which to generate offsets (optional). If not provided, the
                chromosomes will be inferred from the block matrix.

    Returns:
        Pandas DataFrame containing offset values per chromosome. The rows are indexed by sample ID and
        chromosome; the columns are indexed by label. The column types are float64. The DataFrame is sorted using
        chromosome as the primary sort key, and sample ID as the secondary sort key.
    """
    ridge_reduced = RidgeReduction(block_df, label_df, sample_blocks, cov_df, add_intercept,
                                   reduction_alphas, label_type)

    ridge_reduced.fit_transform().cache()
    if ridge_reduced.is_binary():
        regression = LogisticRidgeRegression.from_ridge_reduction(ridge_reduced, regression_alphas)
        return regression.fit_transform_loco('linear', chromosomes)
    else:
        regression = RidgeRegression.from_ridge_reduction(ridge_reduced, regression_alphas)
        return regression.fit_transform_loco(chromosomes)
