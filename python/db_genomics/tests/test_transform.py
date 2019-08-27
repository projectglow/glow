import pytest
from pyspark.sql.utils import IllegalArgumentException
import db_genomics as sg


def test_transform(spark):
    df = spark.read.format("com.databricks.vcf")\
        .load("test-data/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf")
    converted = sg.transform("pipe", df, inputFormatter="vcf", outputFormatter="vcf",
                             cmd='["cat"]', in_vcfHeader="infer")
    assert converted.count() == 1075


def test_no_transform(spark):
    df = spark.read.format("com.databricks.vcf") \
        .load("test-data/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf")
    with pytest.raises(IllegalArgumentException):
        sg.transform("dne", df)


def test_arg_map(spark):
    df = spark.read.format("com.databricks.vcf") \
        .load("test-data/CEUTrio.HiSeq.WGS.b37.NA12878.20.21.vcf")
    args = {
        "inputFormatter": "vcf",
        "outputFormatter": "vcf",
        "cmd": '["cat"]',
        "in_vcfHeader": "infer"
    }
    converted = sg.transform("pipe", df, args)
    assert converted.count() == 1075
