from pyspark.sql import SparkSession
import pytest


# Shared across Python and docs tests
@pytest.fixture(scope="session")
def spark():
    sess = SparkSession.builder \
        .master("local[2]") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.driver.maxResultSize", "0") \
        .config("spark.kryo.registrator", "org.broadinstitute.hellbender.engine.spark.GATKRegistrator") \
        .config("spark.kryoserializer.buffer.max", "2047m") \
        .config("spark.kryo.registrationRequired", "false") \
        .config(
        "spark.hadoop.io.compression.codecs",
        "org.seqdoop.hadoop_bam.util.BGZFCodec,org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec") \
        .getOrCreate()
    yield sess
    sess.stop()
