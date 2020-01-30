from pyspark.sql import SparkSession
import pytest

# Shared across Python and docs tests
@pytest.fixture(scope="module")
def spark():
    sess = SparkSession.builder \
        .master("local[2]") \
        .config("spark.hadoop.io.compression.codecs", "io.projectglow.sql.util.BGZFCodec") \
        .getOrCreate()
    return sess.newSession()
