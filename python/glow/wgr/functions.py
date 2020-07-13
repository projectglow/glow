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
from pyspark import SparkContext
from pyspark.sql import DataFrame, Row, SparkSession, SQLContext
from typeguard import check_argument_types, check_return_type
from typing import Dict, List


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
        - Are in `genotype.sampleId`
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
    distinct_num_values = variant_df.selectExpr("size(values) as numValues").distinct()
    distinct_num_values_count = distinct_num_values.count()
    if distinct_num_values_count == 0:
        raise Exception("DataFrame has no values.")
    if distinct_num_values_count > 1:
        raise Exception("Each row must have the same number of values.")
    num_values = distinct_num_values.head().numValues
    if num_values != len(sample_ids):
        raise Exception("Number of values does not match between DataFrame and sample ID list.")
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
        Row(label='label1', contigName='chr1', values=[1])

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
        ordered_cols = pd.unique(label_df.index.get_level_values(0))
        transposed_df = label_df.T.stack()[ordered_cols]
        column_names = ['label', 'contigName', 'values']
    else:
        raise ValueError('label_df must be indexed by sample id or by (sample id, contig name)')

    transposed_df['values_array'] = transposed_df.to_numpy().tolist()
    return spark.createDataFrame(transposed_df[['values_array']].reset_index(), column_names)
