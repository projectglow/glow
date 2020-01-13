import pytest
from pyspark.sql.functions import expr, lit
from pyspark.sql.utils import IllegalArgumentException
import glow


def test_transform(spark):
    df = spark.read.format("vcf")\
        .load("test-data/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf")
    converted = glow.transform("pipe", df, input_formatter="vcf", output_formatter="vcf",
                             cmd='["cat"]', in_vcf_header="infer")
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
