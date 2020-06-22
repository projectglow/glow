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
from pyspark import SparkContext
from pyspark.sql import DataFrame, Row, SQLContext
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
    output_df.printSchema()
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
