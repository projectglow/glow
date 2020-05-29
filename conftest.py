from pyspark.sql import SparkSession
import pytest

# Set up a new Spark session for each test suite
@pytest.fixture(scope="module")
def spark():
    print("set up new spark session")
    sess = SparkSession.builder \
        .master("local[2]") \
        .config("spark.hadoop.io.compression.codecs", "io.projectglow.sql.util.BGZFCodec") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    return sess.newSession()
