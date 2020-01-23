from pyspark.sql import SparkSession
import pytest


# Shared across Python and docs tests
@pytest.fixture(scope="session")
def spark():
    sess = SparkSession.builder \
        .master("local[2]") \
        .config(
            "spark.hadoop.io.compression.codecs",
            "org.seqdoop.hadoop_bam.util.BGZFCodec,org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec") \
        .getOrCreate()
    yield sess
    sess.stop()
