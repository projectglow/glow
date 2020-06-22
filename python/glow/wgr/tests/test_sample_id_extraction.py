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

import pytest
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException
from glow.wgr import functions


def __construct_row(sample_id_1, sample_id_2):
    return Row(contigName="chr21",
               genotypes=[
                   Row(sampleId=sample_id_1, calls=[1, 1]),
                   Row(sampleId=sample_id_2, calls=[0, 1])
               ])


def test_get_sample_ids(spark):
    df = spark.read.format("vcf").load("test-data/combined.chr20_18210071_18210093.g.vcf")
    sample_ids = functions.get_sample_ids(df)
    assert (sample_ids == ["HG00096", "HG00268", "NA19625"])


def test_missing_sample_id_field(spark):
    df = spark.read.format("vcf").option("includeSampleIds", "false") \
        .load("test-data/combined.chr20_18210071_18210093.g.vcf")
    with pytest.raises(AnalysisException):
        functions.get_sample_ids(df)


def test_inconsistent_sample_ids(spark):
    df = spark.createDataFrame([__construct_row("a", "b"), __construct_row("a", "c")])
    with pytest.raises(Exception):
        functions.get_sample_ids(df)


def test_incorrectly_typed_sample_ids(spark):
    df = spark.createDataFrame([__construct_row(1, 2)])
    with pytest.raises(Exception):
        functions.get_sample_ids(df)


def test_empty_sample_ids(spark):
    df = spark.createDataFrame([__construct_row("a", "")])
    with pytest.raises(Exception):
        functions.get_sample_ids(df)


def test_duplicated_sample_ids(spark):
    df = spark.createDataFrame([__construct_row("a", "a")])
    with pytest.raises(Exception):
        functions.get_sample_ids(df)
