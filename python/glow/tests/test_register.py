import pytest
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException
import glow as glow


def test_no_register(spark):
    df = spark.read.format("vcf") \
        .load("test-data/1kg_sample.vcf")
    with pytest.raises(AnalysisException):
        df.selectExpr("expand_struct(dp_summary_stats(genotypes))")


def test_register(spark):
    glow.register(spark)
    df = spark.read.format("vcf") \
        .load("test-data/1kg_sample.vcf")
    stats = df.selectExpr("expand_struct(dp_summary_stats(genotypes))") \
            .select("min", "max") \
            .head()
    assert stats.asDict() == Row(min=1.0, max=23).asDict()
