import pytest
from pyspark.sql import functions, Row
import glow

@pytest.fixture(autouse=True)
def add_spark(doctest_namespace, spark):
    glow.register(spark)
    doctest_namespace['Row'] = Row
    doctest_namespace['spark'] = spark
    doctest_namespace['lit'] = functions.lit
    doctest_namespace['col'] = functions.col
    doctest_namespace['glow'] = glow

