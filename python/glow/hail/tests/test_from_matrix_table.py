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
def _compare_struct_types(s1, s2, ignore_fields=[]):
    s1_fields = [f for f in s1.fields if f.name not in ignore_fields]
    s2_fields = [f for f in s2.fields if f.name not in ignore_fields]
    assert set([f.name for f in s1_fields]) == set([f.name for f in s2_fields])
    for f1 in s1_fields:
        matching_fields = [f2 for f2 in s2_fields if f1.name == f2.name]
        assert (len(matching_fields) == 1)
        m = matching_fields[0]
        if isinstance(m.dataType, ArrayType) and isinstance(m.dataType.elementType, StructType):
            _compare_struct_types(f1.dataType.elementType, m.dataType.elementType, ignore_fields)
        else:
            assert f1.dataType == m.dataType


def _assert_lossless_adapter(spark,
                             tmp_path,
                             hail_df,
                             input_file,
                             in_fmt,
                             out_fmt,
                             writer_options={},
                             reader_options={}):
    # Convert Hail MatrixTable to Glow DataFrame and write it to a flat file
    output_file = (tmp_path / 'tmp').as_uri() + '.' + in_fmt
    writer = hail_df.write.format(out_fmt)
    for key, value in writer_options.items():
        writer = writer.option(key, value)
    writer.save(output_file)

    # Assert that reread DF has the same schema (excluding metadata/order)
    reader = spark.read.format(in_fmt)
    for key, value in reader_options.items():
        reader = reader.option(key, value)
    round_trip_df = reader.load(output_file)
    glow_df = reader.load(input_file)
    _compare_struct_types(glow_df.schema, round_trip_df.schema)

    # Assert that no data is lost
    matching_df = spark.read.format(in_fmt).schema(glow_df.schema).load(output_file)
    assert matching_df.subtract(glow_df).count() == 0
    assert glow_df.subtract(matching_df).count() == 0


def test_vcf(spark, tmp_path):
    input_vcf = 'test-data/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf'
    hail_df = functions.from_matrix_table(hl.import_vcf(input_vcf))
    _assert_lossless_adapter(spark, tmp_path, hail_df, input_vcf, 'vcf', 'vcf')


def test_gvcf(spark, tmp_path):
    input_vcf = 'test-data/NA12878_21_10002403.g.vcf'
    hail_df = functions.from_matrix_table(hl.import_vcf(input_vcf))
    _assert_lossless_adapter(spark, tmp_path, hail_df, input_vcf, 'vcf', 'vcf')


def test_gvcfs(spark, tmp_path):
    # GVCF MatrixTables are not keyed by locus and alleles, just by locus
    input_vcf = 'test-data/tabix-test-vcf/combined.chr20_18210071_18210093.g.vcf.gz'
    partitions = [
        hl.Interval(hl.Locus("chr20", 1, reference_genome='GRCh38'),
                    hl.Locus("chr20", 20000000, reference_genome='GRCh38'),
                    includes_end=True)
    ]
    hail_df = functions.from_matrix_table(
        hl.import_gvcfs([input_vcf], partitions, force_bgz=True, reference_genome='GRCh38')[0])
    _assert_lossless_adapter(spark, tmp_path, hail_df, input_vcf, 'vcf', 'bigvcf')


def test_annotated_sites_only_vcf(spark, tmp_path):
    # The Hail DataFrame will not have the split CSQ/ANN fields, as it does not have
    # the VCF header metadata; we include the header when writing the round-trip VCF.
    input_vcf = 'test-data/vcf/vep.vcf'
    hail_df = functions.from_matrix_table(hl.import_vcf(input_vcf))
    _assert_lossless_adapter(spark,
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
        _compare_struct_types(hail_df.schema, hail_with_sample_id_df.schema)
    _assert_lossless_adapter(spark,
                             tmp_path,
                             hail_df,
                             input_vcf,
                             'vcf',
                             'vcf',
                             reader_options={'includeSampleIds': 'false'})


def test_unphased_bgen(spark, tmp_path):
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
    input_bgen = 'test-data/bgen/example.8bits.bgen'
    hl.index_bgen(input_bgen, reference_genome=None)
    hail_df = functions.from_matrix_table(hl.import_bgen(input_bgen, entry_fields=['GP']))
    _assert_lossless_adapter(spark,
                             tmp_path,
                             hail_df,
                             input_bgen,
                             'bgen',
                             'bigbgen',
                             writer_options={'bitsPerProbability': '8'})


def test_plink(spark):
    input_base = 'test-data/plink/five-samples-five-variants/bed-bim-fam/test'
    # Do not recode contigs (eg. 23 -> X)
    hail_df = functions.from_matrix_table(
        hl.import_plink(bed=input_base + '.bed',
                        bim=input_base + '.bim',
                        fam=input_base + '.fam',
                        reference_genome=None,
                        contig_recoding={}))

    # Hail does not set the genotype if it is missing; the Glow PLINK reader sets the calls to (-1, -1)
    # Hail sets the genotype phased=False when reading from PLINK if the genotype is present;
    # the Glow PLINK reader does not as it is always false
    glow_df = spark.read.format('plink') \
        .option('mergeFidIid', 'false') \
        .load(input_base + '.bed')
    _compare_struct_types(hail_df.schema, glow_df.schema, ignore_fields=['phased'])
    matching_glow_df = glow_df.withColumn(
        'genotypes',
        fx.expr(
            "transform(genotypes, gt -> named_struct('sampleId', gt.sampleId, 'calls', ifnull(gt.calls, array(-1,-1)), 'phased', if(gt.calls = array(-1, -1), null, false)))"
        ))
    matching_hail_df = hail_df.select(*glow_df.schema.names)
    assert matching_hail_df.subtract(matching_glow_df).count() == 0
    assert matching_glow_df.subtract(matching_hail_df).count() == 0


def test_missing_locus():
    input_vcf = 'test-data/1kg_sample.vcf'
    mt = hl.import_vcf(input_vcf).key_rows_by('alleles').drop('locus')
    with pytest.raises(ValueError):
        functions.from_matrix_table(mt)


def test_missing_alleles():
    input_vcf = 'test-data/1kg_sample.vcf'
    mt = hl.import_vcf(input_vcf).key_rows_by('locus').drop('alleles')
    with pytest.raises(ValueError):
        functions.from_matrix_table(mt)
