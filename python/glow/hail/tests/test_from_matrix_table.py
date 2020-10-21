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
from pyspark.sql.types import ArrayType, StructType
import pytest


# Check that structs have the same fields and datatypes (not necessarily metadata), in any order
def __compare_struct_types(s1, s2):
    assert set([f.name for f in s1.fields]) == set([f.name for f in s2.fields])
    for f1 in s1.fields:
        matching_fields = [f2 for f2 in s2.fields if f1.name == f2.name]
        assert (len(matching_fields) == 1)
        m = matching_fields[0]
        if isinstance(m.dataType, ArrayType) and isinstance(m.dataType.elementType, StructType):
            __compare_struct_types(f1.dataType.elementType, m.dataType.elementType)
        else:
            assert f1.dataType == m.dataType


def __compare_round_trip(spark,
                         tmp_path,
                         hail_df,
                         input_file,
                         in_fmt,
                         out_fmt,
                         writer_options={},
                         reader_options={}):
    # Convert Hail MatrixTable to Glow DataFrame, write it to a flat file, and read it back in
    output_file = (tmp_path / 'tmp').as_uri()
    writer = hail_df.write.format(out_fmt)
    for key, value in writer_options.items():
        writer = writer.option(key, value)
    writer.save(output_file)

    # Assert that round-trip DF has the same schema (excluding metadata/order)
    reader = spark.read.format(in_fmt)
    for key, value in reader_options.items():
        reader = reader.option(key, value)
    round_trip_df = reader.load(output_file)
    glow_df = reader.load(input_file)
    glow_df.printSchema()
    glow_df.show()
    round_trip_df.printSchema()
    round_trip_df.show()
    __compare_struct_types(glow_df.schema, round_trip_df.schema)

    # Assert that no data is lost
    matching_df = spark.read.format(in_fmt).mode('validation').schema(
        glow_df.schema).load(output_file)
    matching_df.show()
    glow_df.show()
    assert matching_df.subtract(glow_df).count() == 0


def test_vcf(spark, tmp_path):
    input_vcf = 'test-data/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf'
    hail_df = functions.from_matrix_table(hl.import_vcf(input_vcf))
    __compare_round_trip(spark, tmp_path, hail_df, input_vcf, 'vcf', 'vcf')


def test_gvcf(spark, tmp_path):
    input_vcf = 'test-data/NA12878_21_10002403.g.vcf'
    hail_df = functions.from_matrix_table(hl.import_vcf(input_vcf))
    __compare_round_trip(spark, tmp_path, hail_df, input_vcf, 'vcf', 'vcf')


def test_annotated_sites_only_vcf(spark, tmp_path):
    # The Hail DataFrame will not have the split CSQ/ANN fields, as it does not have
    # the VCF header metadata; we include the header when writing the round-trip VCF.
    input_vcf = 'test-data/vcf/vep.vcf'
    hail_df = functions.from_matrix_table(hl.import_vcf(input_vcf))
    __compare_round_trip(spark,
                         tmp_path,
                         hail_df,
                         input_vcf,
                         'vcf',
                         'vcf',
                         writer_options={'vcfHeader': input_vcf})


def test_exclude_sample_ids(spark, tmp_path):
    input_vcf = 'test-data/NA12878_21_10002403.vcf'
    hail_df = functions.from_matrix_table(hl.import_vcf(input_vcf), include_sample_ids=False)
    hail_with_sample_id_df = functions.from_matrix_table(hl.import_vcf(input_vcf))
    with pytest.raises(AssertionError):
        __compare_struct_types(hail_df.schema, hail_with_sample_id_df.schema)
    __compare_round_trip(spark,
                         tmp_path,
                         hail_df,
                         input_vcf,
                         'vcf',
                         'vcf',
                         reader_options={'includeSampleIds': 'false'})


def test_bgen(spark, tmp_path):
    input_bgen = 'test-data/bgen/example.8bits.bgen'
    hl.index_bgen(input_bgen, reference_genome=None)
    hail_df = functions.from_matrix_table(hl.import_bgen(input_bgen, entry_fields=['GT', 'GP']))
    hail_df.printSchema()
    hail_df.show()
    __compare_round_trip(spark,
                         tmp_path,
                         hail_df,
                         input_bgen,
                         'bgen',
                         'bigbgen',
                         writer_options={'bitsPerProbability': '8'})
