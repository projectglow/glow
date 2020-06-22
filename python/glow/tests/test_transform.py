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
from pyspark.sql.utils import IllegalArgumentException
import glow


def test_transform(spark):
    df = spark.read.format("vcf")\
        .load("test-data/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf")
    converted = glow.transform("pipe",
                               df,
                               input_formatter="vcf",
                               output_formatter="vcf",
                               cmd='["cat"]',
                               in_vcf_header="infer")
    assert converted.count() == 1075


def test_no_transform(spark):
    df = spark.read.format("vcf") \
        .load("test-data/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf")
    with pytest.raises(IllegalArgumentException):
        glow.transform("dne", df)


def test_arg_map(spark):
    df = spark.read.format("vcf") \
        .load("test-data/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf")
    args = {
        "inputFormatter": "vcf",
        "outputFormatter": "vcf",
        "cmd": '["cat"]',
        "in_vcfHeader": "infer"
    }
    converted = glow.transform("pipe", df, args)
    assert converted.count() == 1075


def test_non_string_arg(spark):
    df = spark.read.format("vcf")\
        .load("test-data/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf")
    converted = glow.transform("pipe",
                               df,
                               input_formatter="vcf",
                               output_formatter="vcf",
                               cmd=["cat"],
                               in_vcf_header="infer")
    assert converted.count() == 1075


def test_non_string_arg_map(spark):
    df = spark.read.format("vcf") \
        .load("test-data/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf")
    args = {
        "inputFormatter": "vcf",
        "outputFormatter": "vcf",
        "cmd": ["cat"],
        "in_vcfHeader": "infer"
    }
    converted = glow.transform("pipe", df, args)
    assert converted.count() == 1075
