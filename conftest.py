from pyspark.sql import SparkSession
import pytest

# Set up a new Spark session for each test suite
@pytest.fixture(scope="module")
def spark():
    sess = SparkSession.builder \
        .master("local[2]") \
        .config("spark.hadoop.io.compression.codecs", "io.projectglow.sql.util.BGZFCodec") \
        .getOrCreate()
    return sess.newSession()
