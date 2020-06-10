import pytest
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException
from glow.levels import functions


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
