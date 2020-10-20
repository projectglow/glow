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

from glow.hail import functions
import hail as hl
from pyspark.sql import functions as fx


def test_annotated_vcf(spark):
    hl.init(spark.sparkContext, idempotent=True, quiet=True)
    input_vcf = 'test-data/vcf/vep.vcf'
    hail_df = functions.from_matrix_table(hl.import_vcf(input_vcf))
    glow_df = spark.read.format('vcf').load(input_vcf)
    hail_df.show()
    glow_df.show()
    assert(hail_df.subtract(glow_df.drop("splitFromMultiAllelic")).count() == 0)
