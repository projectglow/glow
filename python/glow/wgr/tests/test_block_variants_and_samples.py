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
from glow.wgr import functions
import pytest
from pyspark.sql import Row
from pyspark.sql.functions import expr
from pyspark.sql.utils import AnalysisException


def __construct_row(values):
    return Row(contigName="chr21",
               start=100,
               referenceAllele="A",
               alternateAlleles=["T", "C"],
               values=values)


def test_block_variants_and_samples(spark):
    variant_df = spark.read.format("vcf") \
        .load("test-data/combined.chr20_18210071_18210093.g.vcf") \
        .withColumn("values", expr("genotype_states(genotypes)"))
    sample_ids = ["HG00096", "HG00268", "NA19625"]
    block_gt, index_map = functions.block_variants_and_samples(variant_df,
                                                               sample_ids,
                                                               variants_per_block=10,
                                                               sample_block_count=2)
    expected_block_gt = glow.transform("block_variants_and_samples",
                                       variant_df,
                                       variants_per_block=10,
                                       sample_block_count=2)
    assert block_gt.collect() == expected_block_gt.collect()
    assert index_map == {"1": ["HG00096", "HG00268"], "2": ["NA19625"]}


def test_missing_values(spark):
    variant_df = spark.read.format("vcf").load("test-data/combined.chr20_18210071_18210093.g.vcf")
    sample_ids = ["HG00096", "HG00268", "NA19625"]
    with pytest.raises(AnalysisException):
        functions.block_variants_and_samples(variant_df,
                                             sample_ids,
                                             variants_per_block=10,
                                             sample_block_count=2)


def test_no_values(spark):
    variant_df = spark.createDataFrame([__construct_row([0, 1])]).limit(0)
    sample_ids = ["a", "b"]
    with pytest.raises(Exception):
        functions.block_variants_and_samples(variant_df,
                                             sample_ids,
                                             variants_per_block=10,
                                             sample_block_count=2)


def test_inconsistent_num_values(spark):
    variant_df = spark.createDataFrame([__construct_row([0, 1]), __construct_row([1, 1, 2])])
    sample_ids = ["a", "b", "c"]
    with pytest.raises(Exception):
        functions.block_variants_and_samples(variant_df,
                                             sample_ids,
                                             variants_per_block=10,
                                             sample_block_count=2)


def test_mismatch_num_values_sample_ids(spark):
    variant_df = spark.createDataFrame([__construct_row([0, 1]), __construct_row([1, 1])])
    sample_ids = ["a", "b", "c"]
    with pytest.raises(Exception):
        functions.block_variants_and_samples(variant_df,
                                             sample_ids,
                                             variants_per_block=10,
                                             sample_block_count=2)


def test_missing_sample_ids(spark):
    variant_df = spark.createDataFrame([__construct_row([0, 1]), __construct_row([1, 1])])
    sample_ids = ["a", ""]
    with pytest.raises(Exception):
        functions.block_variants_and_samples(variant_df,
                                             sample_ids,
                                             variants_per_block=10,
                                             sample_block_count=2)


def test_duplicated_sample_ids(spark):
    variant_df = spark.createDataFrame([__construct_row([0, 1]), __construct_row([1, 1])])
    sample_ids = ["a", "a"]
    with pytest.raises(Exception):
        functions.block_variants_and_samples(variant_df,
                                             sample_ids,
                                             variants_per_block=10,
                                             sample_block_count=2)
